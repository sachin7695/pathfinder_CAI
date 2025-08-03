from vllm import LLM, SamplingParams
import re, json
import os

class llm_context:
    def __init__(self, model_id="Qwen/Qwen2.5-7B-Instruct-GPTQ-Int8"):
        self.system_prompt = ""
        
        # Initialize vLLM model
        self.model = LLM(
            model=model_id,
            trust_remote_code=True,
            dtype="bfloat16",  # or "float16" 
            gpu_memory_utilization=0.9,  # Adjust based on your GPU memory
            max_model_len=4096,  # Adjust based on your context length needs
            tensor_parallel_size=1,  # Set to number of GPUs if using multiple
        )
        
        # Set up sampling parameters (equivalent to your original generation settings)
        self.sampling_params = SamplingParams(
            max_tokens=4096,
            temperature=0.0,  # Equivalent to do_sample=False (greedy decoding)
            top_p=1.0,
            use_beam_search=False,
        )
        
    def __call__(self, transcription):
        prompt = self.system_prompt + transcription        
        formatted_prompt = f"<|im_start|>user\n{prompt}<|im_end|>\n<|im_start|>assistant\n"
        
        # Generate response using vLLM
        outputs = self.model.generate([formatted_prompt], self.sampling_params)
        
        # Extract the generated text from the output
        # vLLM returns only the newly generated tokens, not the full prompt
        generated_text = outputs[0].outputs[0].text.strip()
        
        # Extract JSON from the response
        model_response = self.extract_json(generated_text)
        return model_response

    def extract_json(self, text):
        match = re.search(r'\{.*?\}', text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
        raise ValueError("No valid JSON found")