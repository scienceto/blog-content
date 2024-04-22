import json
import requests

def fetch_all_commits(repo_owner, repo_name, access_token):
    # GitHub API endpoint for fetching commits
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/commits"

    # Headers with authentication token
    headers = {
        'Authorization': f'token {access_token}'
    }
    params = {
        'sha': 'dev',
    }

    all_commits = []

    try:
        while url:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raise an exception for 4XX and 5XX status codes
            commits = response.json()
            all_commits.extend(commits)
            # Check if there are more pages
            url = response.links.get('next', {}).get('url') if 'next' in response.links else None
        return all_commits
    except requests.exceptions.RequestException as e:
        print(f"Error fetching commits: {e}")
        return None
    
def fetch_commit_files(repo_owner, repo_name, commit_sha, access_token):
    # GitHub API endpoint for fetching commit files
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/commits/{commit_sha}"

    # Headers with authentication token
    headers = {
        'Authorization': f'token {access_token}'
    }

    all_files = []

    try:
        while url:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an exception for 4XX and 5XX status codes
            commit_details = response.json()
            files = commit_details['files']
            all_files.extend(files)
            # Check if there are more pages
            url = response.links.get('next', {}).get('url') if 'next' in response.links else None
        return all_files
    except requests.exceptions.RequestException as e:
        print(f"Error fetching commit files: {e}")
        return None

# Entrypoint
if __name__ == "__main__":
    repo_owner = 'scienceto'
    repo_name = 'sim'
    access_token = 'YOUR_GITHUB_TOKEN'

    # Fetch all commits
    commits = fetch_all_commits(repo_owner, repo_name, access_token)
    json.dump(commits, open("commits.json", "w"))

    # Load commits from file (for testing)
    commits = json.load(open("commits.json"))

    if commits:
        commit_report = {}
        for commit in commits:
            author_name = commit['commit']['author']['name']
            commit_sha = commit['sha']

            # Fetch commit files to calculate additions and deletions
            files = fetch_commit_files(repo_owner, repo_name, commit_sha, access_token)

            if files:
                additions = 0
                deletions = 0
                for file in files:
                    additions += file['additions']
                    deletions += file['deletions']

                total_changes = additions + deletions

                if author_name not in commit_report:
                    commit_report[author_name] = {
                        'num_commits': 1,
                        'total_changes': total_changes,
                        'additions': additions,
                        'deletions': deletions
                    }
                else:
                    commit_report[author_name]['num_commits'] += 1
                    commit_report[author_name]['total_changes'] += total_changes
                    commit_report[author_name]['additions'] += additions
                    commit_report[author_name]['deletions'] += deletions

        print("Commit Report:")
        for author, data in commit_report.items():
            print(f"{author}:")
            print(f"  Number of Commits: {data['num_commits']}")
            print(f"  Total Changes: {data['total_changes']} (+{data['additions']}, -{data['deletions']})")
    else:
        print("No commits found or error occurred.")

    json.dump(commit_report, open("commit_report.json", "w"))
