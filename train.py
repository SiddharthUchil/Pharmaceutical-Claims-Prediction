from claims.pipeline.training import TrainingPipeline
from claims.entity.config_entity import TrainingPipelineConfig

if __name__=="__main__":
    training_pipeline_config= TrainingPipelineConfig()
    training_pipeline = TrainingPipeline(training_pipeline_config=training_pipeline_config)
    training_pipeline.start()