from src.embedding.base import EmbeddingStrategy

class EmbeddingFactory:
    @staticmethod
    def load_strategy(config: dict) -> EmbeddingStrategy:
        embedding_config = config["embedding"]
        provider = embedding_config["active_provider"]
        active_model = embedding_config["active_model"]
        
        active_config = embedding_config["providers"][provider][active_model]
        
        match provider:
            case 'fastembed':
                # lazy loading of imports 
                from src.embedding.fast_embed import FastEmbed

                return FastEmbed(active_config, active_model)
            case 'gemini':
                # lazy loading of imports 
                from src.embedding.gemini import GeminiEmbedding

                return GeminiEmbedding(active_config, active_model)

            case _:
                raise ValueError(f"unknown embedding provider: {provider}")



