import os
import logging
import httpx
from typing import Dict, Any

logger = logging.getLogger("reasoning")

class ReasoningLayer:
    def __init__(self):
        # Dynamically pulls the key you set in your terminal window
        self.api_key = os.getenv("GEMINI_API_KEY", "YOUR_PLACEHOLDER_API_KEY_HERE")
        self.model_name = "gemini-2.0-flash"
        self.api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model_name}:generateContent"

    async def analyze_incident(self, context_pack: Dict[str, Any]) -> Dict[str, str]:
        """
        Main entry point called by the orchestrator when a tripwire fires.
        """
        # Tier 1: Quick Signature Check (Fallback if API key is missing)
        if not self.api_key or "AIzaSyBg33LccRL6sQHzWlokS-CYcqg5VdyU9qU" in self.api_key:
            logger.warning("No valid GEMINI_API_KEY found. Falling back to local pattern signatures.")
            return self._tier1_local_patterns(context_pack)

        # Tier 2: Live Cloud AI Inference
        logger.info("Escalating incident context pack to Gemini Cloud AI Core (Tier 2)...")
        return await self._call_gemini_api(context_pack)

    def _tier1_local_patterns(self, context_pack: Dict[str, Any]) -> Dict[str, str]:
        """
        Fast local pattern matcher if the cloud API is unreachable or unconfigured.
        """
        raw_logs_str = str(context_pack.get("logs", ""))
        
        if "WriteConflict" in raw_logs_str:
            return {
                "root_cause": "Database concurrency WriteConflict exception detected locally.",
                "remediation": "Implement an optimistic retry policy with exponential backoff.",
                "tier": "Tier-1 Local Signature"
            }
        
        return {
            "root_cause": "Unknown anomaly detected. Cloud AI layer is unconfigured.",
            "remediation": "Please configure your GEMINI_API_KEY variable to get full AI analysis.",
            "tier": "Tier-1 Fallback"
        }

    async def _call_gemini_api(self, context_pack: Dict[str, Any]) -> Dict[str, str]:
        """
        Executes a real asynchronous HTTP POST request to the Google Gemini API.
        """
        # Construct an engineering system prompt to force structured outputs
        system_instruction = (
            "You are an expert Senior Site Reliability Engineer (SRE). Analyze the provided "
            "JSON context pack containing system logs and cAdvisor metrics. "
            "Identify the single root cause and provide a precise remediation step. "
            "Your output MUST follow this exact plain-text format, with no extra conversation:\n"
            "Root Cause : <One clear sentence>\n"
            "Remediation: <One clear actionable command or fix>"
        )

        user_prompt = f"Analyze this production system incident context pack:\n{context_pack}"

        # Build the official Google AI Studio payload structure
        payload = {
            "contents": [{
                "parts": [{"text": user_prompt}]
            }],
            "systemInstruction": {
                "parts": [{"text": system_instruction}]
            },
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": 150
            }
        }

        headers = {"Content-Type": "application/json"}
        params = {"key": self.api_key}

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    self.api_url, 
                    json=payload, 
                    headers=headers, 
                    params=params, 
                    timeout=30.0
                )
                
                if response.status_code != 200:
                    logger.error(f"Gemini API returned error code {response.status_code}: {response.text}")
                    return self._tier1_local_patterns(context_pack)

                result_json = response.json()
                ai_text = result_json["candidates"][0]["content"]["parts"][0]["text"].strip()
                
                # Parse the custom format back into our display fields
                root_cause = "Unknown anomaly detected by AI core."
                remediation = "Consult standard system operational runbooks."
                
                for line in ai_text.split("\n"):
                    if line.startswith("Root Cause :"):
                        root_cause = line.replace("Root Cause :", "").strip()
                    elif line.startswith("Remediation:"):
                        remediation = line.replace("Remediation:", "").strip()

                return {
                    "root_cause": root_cause,
                    "remediation": remediation,
                    "tier": "Tier-2 Gemini Cloud AI"
                }

            except Exception as e:
                logger.error(f"Failed to communicate with Gemini API: {e}")
                return self._tier1_local_patterns(context_pack)

    @staticmethod
    def _parse_gemini_response(text: str) -> Dict[str, str]:
        """
        Extract Root Cause and Remediation from the LLM's response.
        Falls back gracefully if the model ignores the format constraint.
        """
        root_cause = text.strip()
        remediation = "Consult standard system operational runbooks."

        for line in text.split("\n"):
            if line.startswith("Root Cause :"):
                root_cause = line.replace("Root Cause :", "").strip()
            elif line.startswith("Remediation:"):
                remediation = line.replace("Remediation:", "").strip()

        return {
            "root_cause": root_cause,
            "remediation": remediation,
        }