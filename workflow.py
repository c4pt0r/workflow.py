# workflow.py
# Author: Ed Huang (i@huangdx.net)

# A workflow engine that allows the execution of jobs composed of multiple steps. Each step is defined by an action
# and a set of inputs and outputs.
#
# The job definition can include `on_except` and `on_finish` hooks to handle the job's completion or failure.
# The context is updated during execution with runtime information like `job_run_id` and `job_status`.
# All the system variables are prefixed with `$`.
#
# Example Usage:
#
# ```python
# engine = WorkflowEngine()
#
# @engine.register_action('prepare')
# def prepare(inputs: List[str], context: Context):
#     context['prepared_output'] = "prepared_output.mp4"
#
# @engine.register_action('ffmpeg')
# def ffmpeg(inputs: List[str], context: Context):
#     context['mp3_output'] = "output.mp3"
#
# @engine.register_hook('on_finish')
# def on_finish(context: Context):
#     print("Job finished successfully!")
#
# @engine.register_hook('on_except')
# def on_except(context: Context):
#     print("Job encountered an exception.")
#
# job = {
#     "name": "video_processing",
#     "env": {"input_file": "video.mp4"},
#     "steps": [
#         {
#             "action": "prepare",
#             "input": ["input_file"],
#             "output": ["prepared_output"]
#         },
#         {
#             "action": "ffmpeg",
#             "input": ["prepared_output"],
#             "output": ["mp3_output"]
#         }
#     ],
#     "output": ["mp3_output"],
#     "on_finish": "on_finish",
#     "on_except": "on_except"
# }
#
# with engine:
#     job_future = engine.submit_job(job)
#     try:
#         result = job_future.wait()
#         print(f"Job completed with output: {result}")
#     except Exception as e:
#         print(f"Job failed with exception: {e}")

import concurrent.futures
import inspect
import uuid
from typing import Callable, Dict, List, Any, Optional, Union

# Type definitions
Context = Dict[str, Any]
ActionFunction = Callable[[List[str], Context], None]
JobHook = Callable[[Context], None]

class InvalidActionFunctionError(Exception):
    """Raised when an action function does not match the expected signature."""
    def __init__(self, func_name: str, expected_signature: str):
        super().__init__(f"Function '{func_name}' does not match expected signature: {expected_signature}")

class InvalidHookFunctionError(Exception):
    """Raised when a hook function does not match the expected signature."""
    def __init__(self, func_name: str, expected_signature: str):
        super().__init__(f"Hook function '{func_name}' does not match expected signature: {expected_signature}")

class UnregisteredActionError(Exception):
    """Raised when an action is not registered in the actions registry."""
    def __init__(self, action_name: str):
        super().__init__(f"Action '{action_name}' is not registered.")
        self.action_name = action_name

class UnregisteredHookError(Exception):
    """Raised when a hook is not registered in the hooks registry."""
    def __init__(self, hook_name: str):
        super().__init__(f"Hook '{hook_name}' is not registered.")
        self.hook_name = hook_name

class MissingInputError(Exception):
    """Raised when a required input is missing from the context."""
    def __init__(self, input_name: str):
        super().__init__(f"Required input '{input_name}' is missing from context.")
        self.input_name = input_name

class JobFuture:
    """
    A wrapper around the concurrent.futures.Future object to handle job execution and results.

    Attributes:
        _future (concurrent.futures.Future): The future object associated with the job execution.
        _job (Dict[str, Any]): The job definition.
        _exception (Optional[Exception]): Stores an exception if one occurs during job execution.
    """
    def __init__(self, future: concurrent.futures.Future, job: Dict[str, Any]):
        self._future = future
        self._job = job
        self._exception = None

    def wait(self) -> Union[Dict[str, Any], Exception]:
        """
        Wait for the job to complete and return the outputs specified in the job definition.

        Returns:
            Dict[str, Any]: The outputs as defined in the job's "output" section.
            Exception: If an exception occurred during job execution, it is returned.

        Raises:
            Exception: If an exception occurred during job execution.
        """
        try:
            context = self._future.result()
            if self._exception:
                raise self._exception
            # fill with output
            ret = {output: context.get(output) for output in self._job['output']}
            # fill with system variables
            ret.update({k: v for k, v in context.items() if k.startswith("$")})
            return ret
        except Exception as e:
            if not self._exception:
                self._exception = e
            raise e

class WorkflowEngine:
    """
    A workflow engine that allows the execution of jobs composed of multiple steps. Each step is defined by an action
    and a set of inputs and outputs.

    The job definition can include `on_except` and `on_finish` hooks to handle the job's completion or failure.
    The context is updated during execution with runtime information like `job_run_id` and `job_status`.

    Attributes:
        actions_registry (Dict[str, ActionFunction]): A registry of all registered action functions.
        hooks_registry (Dict[str, JobHook]): A registry of all registered hook functions.
        executor (concurrent.futures.ThreadPoolExecutor): The executor used to run jobs in parallel.
    """

    def __init__(self, max_workers: Optional[int] = None):
        """
        Initialize the WorkflowEngine with an optional maximum number of workers.

        Args:
            max_workers (int, optional): The maximum number of workers for the ThreadPoolExecutor. Defaults to None.
        """
        self.actions_registry: Dict[str, ActionFunction] = {}
        self.hooks_registry: Dict[str, JobHook] = {}
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    
    def __enter__(self):
        """Enter the runtime context related to this object (for use in with statements)."""
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to this object and shut down the executor."""
        self.shutdown()

    def shutdown(self, wait: bool = True):
        """
        Shuts down the executor gracefully.

        Args:
            wait (bool, optional): If True, waits for all running futures to finish. Defaults to True.
        """
        self.executor.shutdown(wait=wait)

    def register_action(self, name: str) -> Callable[[ActionFunction], ActionFunction]:
        """
        A decorator to register an action function under a given name.

        Args:
            name (str): The name to register the action under.

        Returns:
            Callable: A decorator function that registers the action.
        
        Raises:
            InvalidActionFunctionError: If the function signature does not match the expected signature.
        """
        def decorator(func: ActionFunction) -> ActionFunction:
            # Get the function's signature and expected signature
            func_signature = inspect.signature(func)
            expected_signature = inspect.Signature([
                inspect.Parameter('inputs', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=List[str]),
                inspect.Parameter('context', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=Context),
            ])

            # Check if the function signature matches the expected signature
            if func_signature != expected_signature:
                raise InvalidActionFunctionError(func.__name__, str(expected_signature))
            self.actions_registry[name] = func
            return func
        return decorator

    def register_hook(self, name: str) -> Callable[[JobHook], JobHook]:
        """
        A decorator to register a hook function under a given name.

        Args:
            name (str): The name to register the hook under.

        Returns:
            Callable: A decorator function that registers the hook.
        
        Raises:
            InvalidHookFunctionError: If the function signature does not match the expected signature.
        """
        def decorator(func: JobHook) -> JobHook:
            # Get the function's signature and expected signature
            func_signature = inspect.signature(func)
            expected_signature = inspect.Signature([
                inspect.Parameter('context', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=Context),
            ])

            # Check if the function signature matches the expected signature
            if func_signature != expected_signature:
                raise InvalidHookFunctionError(func.__name__, str(expected_signature))
            self.hooks_registry[name] = func
            return func
        return decorator

    def execute_step(self, step: Dict[str, Any], context: Context):
        """
        Executes a single step of a job.

        Args:
            step (Dict[str, Any]): The step to execute, containing the action name, inputs, and outputs.
            context (Context): The context in which to execute the step, storing all inputs and outputs.
        
        Raises:
            UnregisteredActionError: If the action specified in the step is not registered.
            MissingInputError: If any required input for the step is missing in the context.
        """
        action_name = step['action']
        inputs = [context.get(i, i) for i in step['input']]
        outputs = step['output']
        
        # Check if the action is registered
        action = self.actions_registry.get(action_name)
        if not action:
            raise UnregisteredActionError(action_name)
        
        # Check if all required inputs are present in the context
        for input_name in step['input']:
            if input_name not in context:
                raise MissingInputError(input_name)
        
        # Execute the action
        action(inputs, context)
        
        # Update the context with the outputs
        for output in outputs:
            context[output] = context.get(output)

    def execute_job(self, job: Dict[str, Any]) -> Context:
        """
        Executes a job, which consists of multiple steps.

        Args:
            job (Dict[str, Any]): The job definition, including environment variables, steps, and expected outputs.

        Returns:
            Context: The final context after executing all steps of the job.
        
        Raises:
            Exception: If any exception occurs during the job execution, it will be raised and handled.
        """
        context = job['env'].copy()

        # Initialize system variables
        context["$job_run_id"] = str(uuid.uuid4())
        context["$job_status"] = "RUNNING"

        # If user register on_progress function, set it to the context
        if "on_progress" in job and job["on_progress"] in self.hooks_registry:
            context["$on_progress"] = self.hooks_registry[job["on_progress"]]

        try:
            for step in job['steps']:
                context["$current_step"] = step["action"]
                if "$on_progress" in context:
                    context["$on_progress"](context)
                self.execute_step(step, context)
            
            # Mark job as finished
            context["$job_status"] = "FINISHED"

            # Call the on_finish hook if defined
            if "on_finish" in job and job["on_finish"] in self.hooks_registry:
                self.hooks_registry[job["on_finish"]](context)

        except Exception as e:
            # Mark job as failed
            context["$job_status"] = "FAILED"

            # Call the on_except hook if defined
            if "on_except" in job and job["on_except"] in self.hooks_registry:
                self.hooks_registry[job["on_except"]](context)
            # Capture and re-raise the exception
            raise e

        return context

    def submit_job(self, job: Dict[str, Any]) -> JobFuture:
        """
        Submits a job for execution.

        Args:
            job (Dict[str, Any]): The job definition to be submitted.

        Returns:
            JobFuture: A JobFuture object to manage and retrieve the job's result.
        """
        job_future = JobFuture(self.executor.submit(self.execute_job, job), job)
        return job_future

if __name__ == '__main__':
    engine = WorkflowEngine()

    @engine.register_action('prepare')
    def prepare(inputs: List[str], context: Context):
        context['prepared_output'] = "prepared_output.mp4"

    @engine.register_action('ffmpeg')
    def ffmpeg(inputs: List[str], context: Context):
        context['mp3_output'] = "output.mp3"

    @engine.register_hook('on_finish')
    def on_finish(context: Context):
        print("Job finished successfully!")
    
    @engine.register_hook('on_except')
    def on_except(context: Context):
        print("Job encountered an exception.")

    @engine.register_hook('on_progress')
    def on_progress(context: Context):
        print(f"on_progress: Job {context['$job_run_id']}: Step {context['$current_step']} with status {context['$job_status']}")

    job = {
        "name": "video_processing",
        "env": {"input_file": "video.mp4"},
        "steps": [
            {
                "action": "prepare",
                "input": ["input_file"],
                "output": ["prepared_output"]
            },
            {
                "action": "ffmpeg",
                "input": ["prepared_output"],
                "output": ["mp3_output"]
            }
        ],
        "output": ["mp3_output"],
        "on_finish": "on_finish",
        "on_except": "on_except",
        "on_progress": "on_progress"
    }

    with engine:
        job_future = engine.submit_job(job)
        try:
            result = job_future.wait()
            print(f"Job completed with output: {result}")
        except Exception as e:
            print(f"Job failed with exception: {e}")
