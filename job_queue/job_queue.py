# scheduler module poll job queue and launch workflows
from abc import ABC, abstractmethod
import io
import boto3
from datetime import datetime
from typing import Dict, List, Any, Type, get_origin, Callable, Collection
from pydantic import BaseModel, field_serializer, Field, model_serializer
from enum import Enum
import time
from uuid import uuid4
from boto3.dynamodb.conditions import Key, Attr
from threading import Event

"""
Basic data types used in a job
"""
class File(BaseModel):
    """
    A file abstraction that is stored into S3 when stored in job queue
    After reading from job queue, the file will be hydrated as a io bytes buffer 
    """
    Name: str

    # The file content is not serialized into JSON
    content: io.BytesIO = Field(default=None, exclude=True)

    # Allow arbitrary types in your model configuration,  but it doesn't provide any serialization support.
    class Config:
        arbitrary_types_allowed = True


class JobStatus(Enum):
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'

class JobRequest(BaseModel):
    """
    All input data and metadata of a job 
    """
    InputFiles: List[File] = Field(default=[])
    Params: Dict[str, Any] = Field(default={})

class JobResponse(BaseModel):
    """
    All output data and metadata of a job 
    """
    Error: str | None = Field(default=None)
    OutputFiles: List[File] = Field(default=[])



class Job(BaseModel):
    """
    Job request model
    """
    # Metadata
    JobId: str
    JobType: str
    CreatedAt: str
    Status: JobStatus = JobStatus.PENDING

    # Payload
    Request: JobRequest | None
    Response: JobResponse | None

    @field_serializer('Status')
    def serialize_status(self, status: JobStatus, _info):
        return status.value


class JobQueue(ABC):
    """
    Job queue interface implements a simple priority queue for jobs
    """

    @abstractmethod
    def add(self, job: JobRequest) -> Dict[str, Any]:
        """
        add a new job to job queue
        """
        pass

    @abstractmethod
    def get(self, job_id: str, hydrate: bool = False) -> Job:
        """
        get a job from job queue
        """
        pass

    @abstractmethod
    def update(self, job: Job):
        """
        update job status
        """
        pass


    @abstractmethod
    def scan(self, status: str) -> List[Job]:
        """
        scan job queue and return a list of jobs
        """
        pass


    @abstractmethod
    def poll(self) -> List[Job]:
        """
        poll pending jobs from job queue for execution
        this method should be only called by the job scheduler
        """
        pass


class Workflow(ABC):
    """
    Workflow interface
    """
    @abstractmethod
    def __call__(self, request: JobRequest) -> JobResponse:
        pass

    

class JobScheduler(ABC):
    """
    Job scheduler poll job from job queue and launch workers to process jobs
    When a job is completed, the scheduler will update the job status in the job queue
    """

    @abstractmethod
    def run(self):
        """
        run the scheduler
        """
        pass

    @abstractmethod
    def stop(self):
        """
        stop the scheduler
        """
        pass

    @abstractmethod
    def register_workflow(self, name: str, workflow: Workflow):
        """
        register a workflow to the scheduler
        """
        pass


def iterate_file_fields(model: BaseModel, prefix: str = ""):
    if model is None:
        return

    if not (isinstance(model, BaseModel) or isinstance(model, Collection)):
        return

    if isinstance(model, str):
        return

    if isinstance(model, File):
        yield model

    for field_name, field_info in model.model_fields.items():
        # if isinstance(field_info.annotation, type) or get_origin(field_info.annotation) == list or get_origin(field_info.annotation) == dict:
        field = getattr(model, field_name)
        if isinstance(field, File):
            # For non-model fields, add them to the result
            yield field
        elif isinstance(field, Collection):
            for item in field:
                yield from iterate_file_fields(item)
        elif isinstance(field, Dict):
            for key, value in field.items():
                yield from iterate_file_fields(value)
        elif isinstance(field, BaseModel):
            # If the field is another Pydantic model, recurse into it
            if field is not None:
                yield from iterate_file_fields(field)


class DynamoDBJobQueue(JobQueue):
    """
    Job queue implementation using DynamoDB
    """
    def __init__(self, table_name: str, secondary_index_name: str, bucket_name: str):
        self.table_name = table_name
        self.secondary_index_name = secondary_index_name
        self.s3 = boto3.resource('s3')
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)
        self.bucket = self.s3.Bucket(bucket_name)

    def add(self, job_request: JobRequest, job_type: str):
        job = Job(
            JobId=str(uuid4()),
            JobType=job_type,
            Request=job_request,
            Response=None,
            Status=JobStatus.PENDING,
            CreatedAt=datetime.now().isoformat()
        )

        # Always upload binary payload to S3 first
        for file in iterate_file_fields(job_request):
            # upload file to S3
            file_path = f"{job.JobId}/{file.Name}"
            print(f'Uploading file {file_path} to S3')
            self.bucket.upload_fileobj(file.content, file_path)

        # Then insert job metadata into dynamoDB
        response = self.table.put_item(
            Item=job.model_dump()
        )

        return job

    def get(self, job_id: str, hydrate: bool = False):
        # get a job from dynamoDB table
        response = self.table.get_item(
            Key={
                'JobId': job_id
            }        
        )

        job = Job(**response['Item'])

        if hydrate:
            for file in iterate_file_fields(job):
                file_path = f"{job.JobId}/{file.Name}"
                with io.BytesIO() as buffer:
                    self.bucket.download_fileobj(file_path, buffer)
                    file.content = buffer

        return job

    def update(self, job: Job):
        # Upload file to S3
        for file in iterate_file_fields(job):
            file_path = f"{job.JobId}/{file.Name}"
            breakpoint()
            file.content.seek(0)
            self.bucket.upload_fileobj(file.content, file_path)

        self.table.put_item(
            Item=job.model_dump(),
            Expected={
                'JobId': {
                    'Value': job.JobId, 
                    'Exists': True
                }
            }
        )

    def scan(self, status: str = None, index_name: str = None):
        scan_kwargs = {}

        if status :
            scan_kwargs = {
                    'FilterExpression': Attr('Status').eq(status)
            }
        if index_name:
            scan_kwargs['TableName'] = self.table_name
            scan_kwargs['IndexName'] = index_name

        LastEvaluatedKey = None
        jobs = []
        while True:
            if LastEvaluatedKey:
                scan_kwargs['ExclusiveStartKey'] = LastEvaluatedKey
            response = self.table.scan(**scan_kwargs)
            jobs.extend([Job(**item) for item in response['Items']])
            LastEvaluatedKey = response.get('LastEvaluatedKey', None)   
            if not LastEvaluatedKey:
                break

        return jobs

    def poll(self):
        # get all pending jobs from secondary index
        jobs = self.scan(
            status=JobStatus.PENDING.value, 
            index_name=self.secondary_index_name
        )
        return jobs



class SingleThreadJobScheduler(JobScheduler):
    def __init__(self, job_queue: JobQueue):
        self.job_queue = job_queue
        self.stop_event = Event()
        self.workflows = {}
        self.sleep_time = 5


    def run(self):
        while not self.stop_event.is_set():
            jobs = self.job_queue.poll()
            for job in jobs:
                hydrated_job = self.job_queue.get(job.JobId, hydrate=True)
                self._execute_job(hydrated_job)
                time.sleep(self.sleep_time)
                if self.stop_event.is_set():
                    break

    def stop(self):
        self.stop_event.set()

    def register_workflow(self, name: str, workflow: Workflow):
        self.workflows[name] = workflow

    def _execute_job(self, job: Job):
        try:
            job_type = job.JobType
            workflow = self.workflows.get(job_type, None)
            if workflow is None:
                # this workflow job type is not supported by this scheduler
                print(f'Workflow {job_type} not supported')
                return

            response = workflow(job.Request)
            job.Response = response
            job.Status = JobStatus.COMPLETED
            self.job_queue.update(job)
        except Exception as e:
            print(f'Error processing job {job.JobId}: {e}')
            # print stack trace
            import traceback
            traceback.print_exc()
            job.Status = JobStatus.FAILED
            job.Response = JobResponse(Error=str(e))
            self.job_queue.update(job)


class TestWorkflow(Workflow):
    def __call__(self, request: JobRequest) -> JobResponse:
        print(f'Processing job {request}')
        return JobResponse(OutputFiles=[])


class EchoWorkflow(Workflow):
    def __call__(self, request: JobRequest) -> JobResponse:
        print(f'Processing job {request}')
        # Echo the input files
        output_files = []
        for file in request.InputFiles:
            print(f'Echoing file {file.content}')

            new_buffer = io.BytesIO()
            new_buffer.write(file.content.read())
            new_buffer.seek(0)
            output_files.append(
                File(Name=f'{file.Name}-echo', content=new_buffer))

        return JobResponse(OutputFiles=output_files)


if __name__ == '__main__':
    job_queue = DynamoDBJobQueue(
        table_name='xiaoapp-job-queue', 
        secondary_index_name='QueueIndex', 
        bucket_name='xiaoapp-job-data'
    )

    # job = Job(
    #     JobId='1', 
    #     JobType='test', 
    #     CreatedAt=datetime.now().isoformat(), 
    #     Status=JobStatus.PENDING, 
    #     Request=JobRequest(
    #         InputFiles=[
    #             File(Name='file1.txt', content=io.BytesIO(b'Hello, world!')),
    #             File(Name='file2.txt', content=io.BytesIO(b'Hello, world!'))
    #         ]
    #     ),
    #     Response=JobResponse(OutputFiles=[])
    # )
    # new_job = job_queue.add(job)

    scheduler = SingleThreadJobScheduler(job_queue)
    scheduler.register_workflow('echo', EchoWorkflow())
    scheduler.run()

    

    
