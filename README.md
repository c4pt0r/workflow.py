# workflow.py

 A workflow engine that allows the execution of jobs composed of multiple steps. Each step is defined by an action
 and a set of inputs and outputs.

 The job definition can include `on_except` and `on_finish` hooks to handle the job's completion or failure.
 The context is updated during execution with runtime information like `job_run_id` and `job_status`.
 All the system variables are prefixed with `$`.

 Example Usage:

 ```python
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
     "on_except": "on_except"
 }

 with engine:
     job_future = engine.submit_job(job)
     try:
         result = job_future.wait()
         print(f"Job completed with output: {result}")
     except Exception as e:
         print(f"Job failed with exception: {e}")
```
