import csv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
import os
from dotenv import load_dotenv
from datetime import timedelta

# Load environment variables from .env file
load_dotenv()

def get_video_details(youtube, video_id):
    try:
        request = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=video_id
        )
        response = request.execute()

        if 'items' in response and len(response['items']) > 0:
            video = response['items'][0]
            snippet = video['snippet']
            statistics = video['statistics']
            content_details = video['contentDetails']

            # Parse duration
            duration = content_details['duration']
            duration_obj = parse_duration(duration)

            return {
                'comment_count': int(statistics.get('commentCount', 0)),
                'like_count': int(statistics.get('likeCount', 0)),
                'view_count': int(statistics.get('viewCount', 0)),
                'duration': str(duration_obj),
                'description': snippet.get('description', ''),
                'tags': ', '.join(snippet.get('tags', [])),
                'category_id': snippet.get('categoryId', '')
            }
    except HttpError as e:
        print(f"An HTTP error {e.resp.status} occurred: {e.content}")
    return None

def parse_duration(duration):
    """Convert YouTube API duration format to timedelta"""
    duration = duration[2:]  # Remove 'PT' from the beginning
    days, duration = duration.split('D') if 'D' in duration else (0, duration)
    hours, duration = duration.split('H') if 'H' in duration else (0, duration)
    minutes, duration = duration.split('M') if 'M' in duration else (0, duration)
    seconds = duration.split('S')[0] if 'S' in duration else 0
    return timedelta(days=int(days), hours=int(hours), minutes=int(minutes), seconds=int(seconds))

def search_videos_by_keyword(youtube, keyword, max_results):
    videos = []
    next_page_token = None
    
    while len(videos) < max_results:
        try:
            request = youtube.search().list(
                q=keyword,
                part='snippet',
                type='video',
                maxResults=50,  # API maximum per request
                pageToken=next_page_token
            )
            response = request.execute()
            
            for item in response['items']:
                video_title = item['snippet']['title']
                video_id = item['id']['videoId']
                video_url = f"https://www.youtube.com/watch?v={video_id}"
                if 'electrical' in video_title.lower() or 'construction' in video_title.lower():
                    video_details = get_video_details(youtube, video_id)
                    if video_details:
                        videos.append((video_title, video_url, keyword, video_details))
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token or len(videos) >= max_results:
                break
            
            time.sleep(1)  # Respect YouTube API rate limits
        
        except HttpError as e:
            print(f"An HTTP error {e.resp.status} occurred: {e.content}")
            break
    
    return videos[:max_results]

def collect_videos(youtube, keywords, max_results_per_keyword, quota_limit):
    all_videos = []
    for keyword in keywords:
        print(f"Searching for '{keyword}'...")
        videos = search_videos_by_keyword(youtube, keyword, max_results_per_keyword)
        all_videos.extend(videos)
        print(f"Found {len(videos)} videos for '{keyword}'")
        
        if len(all_videos) >= quota_limit:
            print("Approaching daily quota limit. Stopping search.")
            break
    return all_videos

def load_existing_videos(file_path):
    existing_videos = []
    if os.path.exists(file_path):
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                video_details = {
                    'comment_count': int(row.get('Comment Count', 0)),
                    'like_count': int(row.get('Like Count', 0)),
                    'view_count': int(row.get('View Count', 0)),
                    'duration': row.get('Duration', ''),
                    'description': row.get('Description', ''),
                    'tags': row.get('Tags', ''),
                    'category_id': row.get('Category ID', '')
                }
                existing_videos.append((
                    row['Title'], 
                    row['URL'], 
                    row.get('Keyword', ''),  # Keyword might not exist in old format
                    video_details
                ))
    return existing_videos

def remove_duplicates(videos):
    unique_videos = list({video[1]: video for video in videos}.values())
    duplicate_count = len(videos) - len(unique_videos)
    print(f"Duplicate videos removed: {duplicate_count}")
    return unique_videos

def save_to_csv(videos, output_file):
    existing_videos = load_existing_videos(output_file)
    all_videos = existing_videos + videos
    unique_videos = remove_duplicates(all_videos)
    
    videos_with_id = [(i+1, *video) for i, video in enumerate(unique_videos)]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['VideoID', 'Title', 'URL', 'Keyword', 'Comment Count', 'Like Count', 'View Count', 'Duration', 'Description', 'Tags', 'Category ID']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for video in videos_with_id:
            writer.writerow({
                'VideoID': video[0],
                'Title': video[1],
                'URL': video[2],
                'Keyword': video[3],
                'Comment Count': video[4]['comment_count'],
                'Like Count': video[4]['like_count'],
                'View Count': video[4]['view_count'],
                'Duration': video[4]['duration'],
                'Description': video[4]['description'],
                'Tags': video[4]['tags'],
                'Category ID': video[4]['category_id']
            })
    
    print(f"CSV file '{output_file}' updated successfully with {len(videos_with_id)} unique videos.")

def main(output_file, keywords, max_results_per_keyword, quota_limit):
    youtube = build(API_SERVICE_NAME, API_VERSION, developerKey=YOUTUBE_API_KEY)
    new_videos = collect_videos(youtube, keywords, max_results_per_keyword, quota_limit)
    save_to_csv(new_videos, output_file)

if __name__ == "__main__":
    # Configuration section - Adjust these variables as needed
    API_SERVICE_NAME = 'youtube'
    API_VERSION = 'v3'
    YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')  # Make sure this is set in your .env file
    
    # Output file configuration
    output_file_name = 'video_URL_test.csv'
    # For deployment (uncomment these and comment out the test lines above when ready)
    # output_file_name = 'video_URL.csv'
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, output_file_name)
    
    # Search parameters
    keywords = [
        "Electrical troubleshooting in construction",
        "Electrical conduit installation",
        "Commercial electrical construction",
    ]
    MAX_RESULTS_PER_KEYWORD = 3
    QUOTA_LIMIT = 9900
    
    # Run the main function with the configured parameters
    main(output_path, keywords, MAX_RESULTS_PER_KEYWORD, QUOTA_LIMIT)