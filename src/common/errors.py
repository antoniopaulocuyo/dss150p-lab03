class PipelineStageError(Exception):
    """Raised when a pipeline stage fails for system/execution reasons
    (not data-quality reasons — those go to quarantine, not here).

    Carries the stage name and run_id so logs/tracebacks always show
    exactly where and in which run a failure happened.
    """

    def __init__(self, stage: str, run_id: str, message: str):
        self.stage = stage
        self.run_id = run_id
        super().__init__(f"[stage={stage}] [run_id={run_id}] {message}")