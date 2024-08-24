import unittest
from workflow import WorkflowEngine, InvalidActionFunctionError, UnregisteredActionError, MissingInputError, InvalidHookFunctionError, Context
from typing import List

class TestWorkflowEngine(unittest.TestCase):

    def setUp(self):
        """Set up the WorkflowEngine instance before each test."""
        self.engine = WorkflowEngine(max_workers=2)

    def tearDown(self):
        """Shut down the executor after each test."""
        self.engine.shutdown()

    def test_register_action_with_valid_signature(self):
        @self.engine.register_action('valid_action')
        def valid_action(inputs: List[str], context: Context):
            context['result'] = "valid_action_completed"

        self.assertIn('valid_action', self.engine.actions_registry)

    def test_register_action_with_invalid_signature(self):
        with self.assertRaises(InvalidActionFunctionError):
            @self.engine.register_action('invalid_action')
            def invalid_action(input_data: str, context: Context):  # Invalid signature
                context['result'] = "invalid_action_completed"

    def test_register_hook_with_valid_signature(self):
        @self.engine.register_hook('on_finish')
        def on_finish(context: Context):
            context['hook'] = "finished"

        self.assertIn('on_finish', self.engine.hooks_registry)

    def test_register_hook_with_invalid_signature(self):
        with self.assertRaises(InvalidHookFunctionError):
            @self.engine.register_hook('on_finish')
            def on_finish(input_data: str):  # Invalid signature
                print("Finished")

    def test_execute_job_with_valid_steps(self):
        @self.engine.register_action('prepare')
        def prepare(inputs: List[str], context: Context):
            context['prepared_output'] = "prepared_output.mp4"

        @self.engine.register_action('ffmpeg')
        def ffmpeg(inputs: List[str], context: Context):
            context['mp3_output'] = "output.mp3"

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
            "output": ["mp3_output"]
        }

        job_future = self.engine.submit_job(job)
        result = job_future.wait()
        
        self.assertEqual(result.get("mp3_output"), "output.mp3")
        self.assertEqual(result.get("$job_status"), "FINISHED")
        self.assertTrue("$job_run_id" in result)

    def test_execute_job_with_missing_input(self):
        @self.engine.register_action('prepare')
        def prepare(inputs: List[str], context: Context):
            context['prepared_output'] = "prepared_output.mp4"

        job = {
            "name": "video_processing",
            "env": {},  # No 'input_file' in the environment
            "steps": [
                {
                    "action": "prepare",
                    "input": ["input_file"],  # This will cause MissingInputError
                    "output": ["prepared_output"]
                }
            ],
            "output": ["prepared_output"]
        }

        with self.assertRaises(MissingInputError):
            job_future = self.engine.submit_job(job)
            job_future.wait()

    def test_execute_job_with_unregistered_action(self):
        job = {
            "name": "video_processing",
            "env": {"input_file": "video.mp4"},
            "steps": [
                {
                    "action": "unregistered_action",  # This action is not registered
                    "input": ["input_file"],
                    "output": ["result"]
                }
            ],
            "output": ["result"]
        }

        with self.assertRaises(UnregisteredActionError):
            job_future = self.engine.submit_job(job)
            job_future.wait()

    def test_on_finish_hook(self):
        @self.engine.register_action('prepare')
        def prepare(inputs: List[str], context: Context):
            context['prepared_output'] = "prepared_output.mp4"

        @self.engine.register_hook('on_finish')
        def on_finish(context: Context):
            context['hook'] = "finished"

        job = {
            "name": "video_processing",
            "env": {"input_file": "video.mp4"},
            "steps": [
                {
                    "action": "prepare",
                    "input": ["input_file"],
                    "output": ["prepared_output"]
                }
            ],
            "output": ["prepared_output"],
            "on_finish": "on_finish"
        }

        job_future = self.engine.submit_job(job)
        result = job_future.wait()
        
        self.assertEqual(result.get("prepared_output"), "prepared_output.mp4")
        self.assertEqual(result.get("$job_status"), "FINISHED")

    def test_on_except_hook(self):
        @self.engine.register_action('prepare')
        def prepare(inputs: List[str], context: Context):
            raise Exception("Test exception")

        @self.engine.register_hook('on_except')
        def on_except(context: Context):
            context['hook'] = "exception_handled"

        job = {
            "name": "video_processing",
            "env": {"input_file": "video.mp4"},
            "steps": [
                {
                    "action": "prepare",
                    "input": ["input_file"],
                    "output": ["prepared_output"]
                }
            ],
            "output": ["prepared_output"],
            "on_except": "on_except"
        }

        with self.assertRaises(Exception):
            job_future = self.engine.submit_job(job)
            job_future.wait()

    def test_on_progress_hook(self):
        @self.engine.register_action('prepare')
        def prepare(inputs: List[str], context: Context):
            context['prepared_output'] = "prepared_output.mp4"

        @self.engine.register_action('ffmpeg')
        def ffmpeg(inputs: List[str], context: Context):
            context['mp3_output'] = "output.mp3"

        @self.engine.register_hook('on_progress')
        def on_progress(context: Context):
            print(f"Progress: Step {context['$current_step']} with status {context['$job_status']}")

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
            "on_progress": "on_progress"
        }

        job_future = self.engine.submit_job(job)
        result = job_future.wait()

        self.assertEqual(result.get("mp3_output"), "output.mp3")
        self.assertEqual(result.get("$job_status"), "FINISHED")
        self.assertTrue("$job_run_id" in result)
        self.assertEqual(result.get("$current_step"), "ffmpeg")


if __name__ == '__main__':
    unittest.main()

