import csv
import json
import os
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptAvailable
from urllib.parse import urlparse, parse_qs

def extract_video_id(url):
    parsed_url = urlparse(url)
    if parsed_url.hostname in ('youtu.be', 'www.youtu.be'):
        return parsed_url.path[1:]
    if parsed_url.hostname in ('youtube.com', 'www.youtube.com'):
        if parsed_url.path == '/watch':
            return parse_qs(parsed_url.query)['v'][0]
        if parsed_url.path[:7] == '/embed/':
            return parsed_url.path.split('/')[2]
        if parsed_url.path[:3] == '/v/':
            return parsed_url.path.split('/')[2]
    return None

def get_transcript(url):
    video_id = extract_video_id(url)
    if not video_id:
        print(f"Could not extract video ID from URL: {url}")
        return None
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=['en-US', 'en'])
        full_transcript = ' '.join([entry['text'] for entry in transcript])
        return full_transcript
    except (TranscriptsDisabled, NoTranscriptAvailable):
        print(f"No transcript available for video ID: {video_id}")
        return None
    except Exception as e:
        print(f"Error getting transcript for video ID {video_id}: {str(e)}")
        return None

def clean_column_name(name):
    return ''.join(char for char in name if char.isprintable()).strip()

def process_csv_to_json(input_file, output_file):
    print(f"Starting to process {input_file}")
    
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} does not exist.")
        return

    try:
        with open(input_file, 'r', newline='', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            reader.fieldnames = [clean_column_name(field) for field in reader.fieldnames]
            output_data = []
            
            for row in reader:
                url = row['URL']
                video_id = row['VideoID']
                print(f"Processing VideoID: {video_id}")
                
                transcript = get_transcript(url)
                
                if transcript is not None:
                    output_row = {
                        'VideoID': video_id,
                        'Title': row['Title'],
                        'URL': url,
                        'Keyword': row['Keyword'],
                        'Transcript': transcript
                    }
                    output_data.append(output_row)
                    print(f"Successfully extracted transcript for VideoID: {video_id}")
                else:
                    print(f"Skipping VideoID: {video_id} due to missing transcript")

        with open(output_file, 'w', encoding='utf-8') as outfile:
            json.dump(output_data, outfile, indent=2, ensure_ascii=False)

        print(f"Processing complete. Results saved to {output_file}")
    except Exception as e:
        print(f"An error occurred while processing the file: {str(e)}")

# File configuration
script_dir = os.path.dirname(os.path.abspath(__file__))

# For testing
input_file_name = 'video_URL_test.csv'
output_file_name = 'transcripts_test.json'

# For deployment (uncomment these and comment out the test lines above when ready)
# input_file_name = 'video_URL.csv'
# output_file_name = 'transcripts.json'

input_path = os.path.join(script_dir, input_file_name)
output_path = os.path.join(script_dir, output_file_name)

# Process the files
process_csv_to_json(input_path, output_path)

print("Script execution completed.")