from workflowlite import WorkflowEngine, Context, JobHook
from typing import List, Any

# Initialize the workflow engine
engine = WorkflowEngine()

@engine.register_action('load_data')
def load_data(inputs: List[str], context: Context):
    # Simulating data loading
    context['data'] = f"Data loaded from {inputs[0]}"

@engine.register_action('process_data')
def process_data(inputs: List[str], context: Context):
    data = context.get(inputs[0])
    # Simulating data processing
    context['processed_data'] = f"Processed {data}"
    raise Exception("Error occurred during data processing")

@engine.register_action('generate_report')
def generate_report(inputs: List[str], context: Context):
    logger = context.get("$$log", lambda *args: None)
    logger(context, f"Generating {context} report")
    processed_data = context.get(inputs[0])
    # Simulating report generation
    context['report'] = f"Report generated for {processed_data}"

@engine.register_hook('on_finish')
def on_finish(context: Context, extra: Any):
    print(f"Job finished successfully!")
    print(context['report'])  # Display the generated report

@engine.register_hook('on_except')
def on_except(context: Context, extra: Any):
    print(f"Job encountered an exception")

@engine.register_hook('log')
def log(context: Context, extra: Any):
    print(f"Logging: {extra}")

# Define the job
job = {
    "name": "data_processing",
    "env": {"input_file": "data.csv"},
    "steps": [
        {
            "action": "load_data",
            "input": ["input_file"],
            "output": ["data"]
        },
        {
            "action": "process_data",
            "input": ["data"],
            "output": ["processed_data"]
        },
        {
            "action": "generate_report",
            "input": ["processed_data"],
            "output": ["report"]
        }
    ],
    "output": ["report"],
    "on_finish": "on_finish",
    "on_except": "on_except"
}

with engine:
    job_future = engine.submit_job(job)
    try:
        result = job_future.wait()
    except Exception as e:
        print(f"Job failed with exception: {e}")

