import json
import csv
import requests
import time
import os
from typing import List, Dict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def analyze_transcript(transcript: str, api_key: str, model: str) -> str:
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    
    prompt = f"""
    Analyze the following transcript from an electrical construction video and provide insights in this exact structure:

    {{
    "Electrical_Terms": [
        "term1",
        "term2",
        ...
    ],
    "Problems_Challenges": [
        "problem1",
        "problem2",
        ...
    ],
    "Tools_Equipment": [
        "tool1 (brief description if needed)",
        "tool2 (brief description if needed)",
        ...
    ],
    "Educational_Content": [
        "point1",
        "point2",
        ...
    ]
    }}

    Rules:
    1. Use only the categories provided.
    2. Each category should contain a list of items.
    3. Each item should be a short phrase or single sentence.
    4. For Tools_Equipment, include a very brief description (1-3 words) only if necessary for clarity.
    5. Do not include any additional text, explanations, or category labels outside of this structure.

    Transcript:
    {transcript}

    Provide your analysis strictly in the JSON-like structure specified above.
    """
    
    data = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are an expert in electrical construction analyzing video transcripts. Provide concise, structured analysis in the exact format specified."},
            {"role": "user", "content": prompt}
        ]
    }
    
    try:
        response = requests.post('https://api.openai.com/v1/chat/completions', headers=headers, json=data)
        response.raise_for_status()
        
        if response.status_code == 200:
            content = response.json()['choices'][0]['message']['content'].strip()
            json.loads(content)  # Validate JSON
            return content
        else:
            return f"Error: Unexpected status code {response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"Request Error: {str(e)}"
    except json.JSONDecodeError as e:
        return f"JSON Decode Error: {str(e)}"
    except KeyError as e:
        return f"Key Error: {str(e)}"
    except Exception as e:
        return f"Unexpected Error: {str(e)}"

def process_videos(start_id: int, end_id: int, input_file: str, output_file: str, api_key: str, model: str, min_transcript_words: int):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(script_dir, input_file)
    output_file = os.path.join(script_dir, output_file)
    
    try:
        with open(input_file, 'r', encoding='utf-8') as file:
            data = json.load(file)
    except UnicodeDecodeError:
        print(f"UTF-8 decoding failed for {input_file}. Trying with ISO-8859-1 encoding...")
        with open(input_file, 'r', encoding='iso-8859-1') as file:
            data = json.load(file)

    csv_headers = ['VideoID', 'Title', 'URL', 'Electrical Terms', 'Problems/Challenges', 'Tools/Equipment', 'Educational Content']

    processed_video_ids = set()
    file_exists = os.path.isfile(output_file)
    if file_exists:
        with open(output_file, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            processed_video_ids = set(int(row['VideoID']) for row in reader if row['VideoID'].isdigit())

    mode = 'a' if file_exists else 'w'
    with open(output_file, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
        
        if not file_exists:
            writer.writeheader()

        videos_processed = 0
        videos_skipped = 0
        last_request_time = 0
        min_request_interval = 0.1

        for video in data:
            video_id = int(video['VideoID'])
            if start_id <= video_id <= end_id:
                if video_id in processed_video_ids:
                    print(f"Video {video_id} already processed. Skipping.")
                    videos_skipped += 1
                    continue

                title = video['Title']
                url = video['URL']
                transcript = video['Transcript']

                print(f"Processing video: {video_id}")

                if len(transcript.split()) < min_transcript_words:
                    print(f"Video {video_id} has very short or no transcript. Skipping analysis.")
                    writer.writerow({
                        'VideoID': video_id,
                        'Title': title,
                        'URL': url,
                        'Electrical Terms': 'N/A - Short/No Transcript',
                        'Problems/Challenges': 'N/A - Short/No Transcript',
                        'Tools/Equipment': 'N/A - Short/No Transcript',
                        'Educational Content': 'N/A - Short/No Transcript'
                    })
                    videos_processed += 1
                    continue

                try:
                    time_since_last_request = time.time() - last_request_time
                    if time_since_last_request < min_request_interval:
                        time.sleep(min_request_interval - time_since_last_request)

                    analysis = analyze_transcript(transcript, api_key, model)
                    last_request_time = time.time()
                    
                    if analysis.startswith("Error:") or analysis.startswith("Request Error:") or analysis.startswith("JSON Decode Error:") or analysis.startswith("Key Error:") or analysis.startswith("Unexpected Error:"):
                        print(f"Error analyzing video {video_id}: {analysis}")
                        writer.writerow({
                            'VideoID': video_id,
                            'Title': title,
                            'URL': url,
                            'Electrical Terms': 'Error in analysis',
                            'Problems/Challenges': 'Error in analysis',
                            'Tools/Equipment': 'Error in analysis',
                            'Educational Content': 'Error in analysis'
                        })
                        videos_processed += 1
                        continue
                    
                    analysis_dict = json.loads(analysis)
                    
                    writer.writerow({
                        'VideoID': video_id,
                        'Title': title,
                        'URL': url,
                        'Electrical Terms': ', '.join(analysis_dict.get('Electrical_Terms', [])),
                        'Problems/Challenges': ', '.join(analysis_dict.get('Problems_Challenges', [])),
                        'Tools/Equipment': ', '.join(analysis_dict.get('Tools_Equipment', [])),
                        'Educational Content': ', '.join(analysis_dict.get('Educational_Content', []))
                    })

                    print(f"Analysis complete for video: {video_id}")
                    videos_processed += 1

                except Exception as e:
                    print(f"Error processing video {video_id}: {str(e)}")

    print(f"Analysis complete. Processed {videos_processed} new videos, skipped {videos_skipped} already processed videos.")
    print(f"Videos with IDs >= {start_id} and <= {end_id} have been analyzed. Results updated in {output_file}")

if __name__ == "__main__":
    # Configuration variables (easily modifiable)
    START_ID = 0
    END_ID = 3 #3653 for final
    INPUT_FILE = os.path.join('..', '01_download_URL_Transcript_Comment', 'transcripts.json')
    OUTPUT_FILE = 'transcript_4o_mini_test.csv'
    API_KEY = os.getenv('OPENAI_API_KEY')
    MODEL = "gpt-4o-mini"  # Change to "gpt-4o" for final analysis
    MIN_TRANSCRIPT_WORDS = 20  # Minimum words in transcript to be considered for analysis

    # Run the analysis
    process_videos(START_ID, END_ID, INPUT_FILE, OUTPUT_FILE, API_KEY, MODEL, MIN_TRANSCRIPT_WORDS)