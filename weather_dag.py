"""
Simple DAG for fetching weather data from a public API.
This DAG demonstrates basic HTTP requests and data processing in Airflow.
"""
import datetime
import logging
from typing import Dict, Any

import pendulum
import requests
from airflow.sdk import task, DAG
from airflow.utils.context import Context

# Configure logging
logger = logging.getLogger(__name__)


@task
def fetch_weather_data(**context: Context) -> Dict[str, Any]:
    """
    Fetch current weather data from a public API.
    
    Args:
        context: Airflow task context
        
    Returns:
        Dict containing weather information
    """
    # Using a free weather API (OpenMeteo)
    # No API key required, just need coordinates
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 55.7558,  # Moscow coordinates
        "longitude": 37.6176,
        "current": "temperature_2m,relative_humidity_2m,weather_code",
        "timezone": "Europe/Moscow"
    }
    
    logger.info("Fetching weather data from OpenMeteo API")
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        weather_data = response.json()
        
        # Extract current weather info
        current = weather_data.get("current", {})
        temperature = current.get("temperature_2m")
        humidity = current.get("relative_humidity_2m")
        weather_code = current.get("weather_code")
        
        # Simple weather code mapping
        weather_descriptions = {
            0: "Clear sky",
            1: "Mainly clear",
            2: "Partly cloudy",
            3: "Overcast",
            45: "Foggy",
            48: "Depositing rime fog",
            51: "Light drizzle",
            53: "Moderate drizzle",
            55: "Dense drizzle",
            61: "Slight rain",
            63: "Moderate rain",
            65: "Heavy rain",
            71: "Slight snow",
            73: "Moderate snow",
            75: "Heavy snow",
            95: "Thunderstorm"
        }
        
        weather_description = weather_descriptions.get(weather_code, "Unknown")
        
        result = {
            "location": "Moscow",
            "temperature_celsius": temperature,
            "humidity_percent": humidity,
            "weather_description": weather_description,
            "timestamp": current.get("time")
        }
        
        # Print the data (this will appear in Airflow logs)
        logger.info("=== WEATHER DATA ===")
        logger.info(f"Location: {result['location']}")
        logger.info(f"Temperature: {result['temperature_celsius']}°C")
        logger.info(f"Humidity: {result['humidity_percent']}%")
        logger.info(f"Weather: {result['weather_description']}")
        logger.info(f"Time: {result['timestamp']}")
        logger.info("===================")
        
        return result
        
    except requests.RequestException as e:
        logger.error(f"Failed to fetch weather data: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


@task
def print_current_time(**context: Context) -> str:
    """
    Print the current execution time.
    
    Args:
        context: Airflow task context
        
    Returns:
        Current time string
    """
    execution_date = datetime.datetime.now(tz=datetime.UTC)
    current_time = execution_date.strftime("%Y-%m-%d %H:%M:%S UTC")
    
    logger.info("=== CURRENT TIME ===")
    logger.info(f"Execution time: {current_time}")
    logger.info("===================")
    
    return current_time


# DAG definition
with DAG(
    dag_id="simple_weather_dag",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    description="Simple DAG that fetches weather data and prints current time",
    doc_md=__doc__,
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=2),
    }
) as dag:
    
    # Define tasks
    time_task = print_current_time()
    weather_task = fetch_weather_data()
    
    # Task dependencies - run time task first, then weather
    time_task >> weather_task 