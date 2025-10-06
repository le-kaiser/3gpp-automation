import requests
from bs4 import BeautifulSoup
import pandas as pd
import zipfile
import os
from urllib.parse import urljoin
import docx
from datetime import datetime

# --- Configuration ---
BASE_URL = "https://www.3gpp.org/ftp/tsg_ran/TSG_RAN"
SPEC_NUMBER = "38.101-1"
# Using a set for efficient lookups
CLAUSES_DATABASE = {'4.1', '5.3.2', '7.1a'} 
OUTPUT_FILE = "approved_clauses.xlsx"
TEMP_DIR = "temp_files"

# --- Main Functions ---

def get_sorted_meeting_folders(url):
    """
    Fetches and sorts the TSG-RAN meeting folders from the 3GPP website by date.
    """
    print(f"Fetching and sorting meeting folders by date from: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    folders_with_dates = []

    # Find all table rows in the body
    for row in soup.find('tbody').find_all('tr'):
        # Find the link within the row
        link = row.find('a')
        if not link:
            continue

        link_text = link.text.strip()
        # Filter for folder links we are interested in
        if link_text.startswith('TSGR_') and not link.get('class'):
            href = link.get('href')
            if not href:
                continue

            # The date is in the next 'td' element after the link's parent 'td'
            date_td = link.find_parent('td').find_next_sibling('td')
            if not date_td:
                continue

            date_str = date_td.text.strip()
            try:
                # Parse date format like "2025/08/10 22:07"
                mod_date = datetime.strptime(date_str, "%Y/%m/%d %H:%M")
                folders_with_dates.append((mod_date, href))
            except ValueError:
                print(f"Could not parse date '{date_str}' for folder {link_text}")
                continue

    # Sort the list of tuples by date (the first element), descending
    folders_with_dates.sort(key=lambda x: x[0], reverse=True)

    # Extract just the URLs from the sorted list
    sorted_links = [href for mod_date, href in folders_with_dates]
    
    print(f"Found and sorted {len(sorted_links)} meeting folders.")
    return sorted_links

def find_excel_in_docs(docs_url):
    """
    Finds and downloads the main .xlsx file from a meeting's Docs folder.
    """
    print(f"Searching for Excel file in: {docs_url}")
    try:
        response = requests.get(docs_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Could not access {docs_url}. Error: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    excel_link = None
    
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.endswith('.xlsx'):
            excel_link = href
            break

    if not excel_link:
        return None

    try:
        # The link should be a full URL, but we use urljoin for safety
        excel_full_url = urljoin(docs_url, excel_link)
        print(f"Found Excel file: {excel_full_url}")
        
        # Get the filename from the URL
        file_name = os.path.basename(excel_full_url.split('?')[0])
        local_path = os.path.join(TEMP_DIR, file_name)

        print(f"Downloading to: {local_path}")
        with requests.get(excel_full_url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        return local_path
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {excel_link}. Error: {e}")
        return None

def filter_approved_crs(excel_path, spec_number):
    """
    Filters the downloaded Excel file for approved CRs for the specified spec.
    Handles case, whitespace, and formatting issues.
    """
    print(f"Filtering CRs in: {excel_path}")
    try:
        xls = pd.ExcelFile(excel_path)
        if 'CR_Packs_List' not in xls.sheet_names:
            print(f"Error: Sheet 'CR_Packs_List' not found in {excel_path}.")
            return []

        # Load the specific sheet
        df = pd.read_excel(xls, sheet_name='CR_Packs_List')

        # Define column names based on your screenshot
        col_rp = 'CR Pack TDoc'     # Column A
        col_r4 = 'WG Tdoc'          # Column B
        col_status = 'CR Individual TSG decision'  # Column D
        col_spec = 'Spec'           # Column E

        # Verify that all required columns exist
        required_cols = [col_rp, col_r4, col_status, col_spec]
        if not all(col in df.columns for col in required_cols):
            print(f"Error: The sheet in {excel_path} is missing one or more required columns.")
            return []

        # Normalize status and spec columns for safe comparison
        status_clean = df[col_status].astype(str).str.strip().str.lower()
        spec_clean = df[col_spec].astype(str).str.strip()

        # Apply filters
        approved_filter = status_clean == 'approved'  # lowercase
        spec_filter = spec_clean == spec_number      # exact match

        filtered_df = df[approved_filter & spec_filter]

        if filtered_df.empty:
            print("No rows matched the filter criteria.")
            return []

        # Extract data from the filtered rows
        results = []
        for index, row in filtered_df.iterrows():
            rp_number = row[col_rp]
            r4_docs = row[col_r4]
            if isinstance(r4_docs, str):
                for r4_doc in r4_docs.replace(' ', '').split(','):
                    if r4_doc.strip():  # avoid empty strings
                        results.append((str(rp_number), r4_doc.strip()))

        print(f"Found {len(results)} relevant CR(s) in {excel_path}")
        return results

    except Exception as e:
        print(f"An unexpected error occurred while processing {excel_path}: {e}")
        import traceback
        traceback.print_exc()
        return []

def process_rp_archive(docs_url, rp_number, r4_doc_name, clauses_db):
    """
    Downloads the RP archive, extracts it, and searches the R4 doc.
    """
    print(f"Processing archive for RP: {rp_number}, searching for doc: {r4_doc_name}")
    # TODO: Implement ZIP download, extraction, and doc searching
    return False

def search_docx_for_clauses(docx_path, clauses_db):
    """
    Searches a .docx file for the 'Clauses Affected' section and checks against the database.
    """
    print(f"Searching for clauses in: {docx_path}")
    # TODO: Implement python-docx logic
    return False

def main():
    """
    Main function to orchestrate the automation workflow.
    """
    print("Starting 3GPP CR automation script...")
    
    # Create a temporary directory for downloaded files
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)

    # 1. Get all meeting folders, sorted latest first
    meeting_folders = get_sorted_meeting_folders(BASE_URL)
    
    results = []

    # 2. Loop through each meeting
    for folder_url in meeting_folders:
        # 3. Find the main Excel file in the 'Docs' subfolder
        # Add a trailing slash for correct urljoin behavior
        docs_url = urljoin(folder_url + '/', 'Docs/')
        excel_file_path = find_excel_in_docs(docs_url)

        if not excel_file_path:
            print(f"No Excel file found in {docs_url}. Skipping.")
            continue

        # 4. Filter the Excel file for relevant CRs
        relevant_crs = filter_approved_crs(excel_file_path, SPEC_NUMBER)

        # 5. Process each relevant CR
        for rp_number, r4_doc_name in relevant_crs:
            # 6. Find and search the docx within the zip archive
            is_relevant = process_rp_archive(docs_url, rp_number, r4_doc_name, CLAUSES_DATABASE)
            
            if is_relevant:
                print(f"Found relevant clause in {r4_doc_name} from {rp_number}. Recording.")
                results.append({
                    'Meeting_Folder': folder_url,
                    'RP_Number': rp_number,
                    'R4_Document': r4_doc_name
                })

    # 7. Save results to an Excel file
    if results:
        print(f"\nSaving {len(results)} found items to {OUTPUT_FILE}")
        results_df = pd.DataFrame(results)
        results_df.to_excel(OUTPUT_FILE, index=False)
    else:
        print("\nNo relevant CRs found matching the criteria.")

    print("\nScript finished.")


if __name__ == "__main__":
    # main() # Temporarily disabled for testing
    print("--- Live Test: Find and Filter First Available Excel Sheet ---")

    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)

    meeting_folders = get_sorted_meeting_folders(BASE_URL)
    
    if not meeting_folders:
        print("Test failed: Could not retrieve any meeting folders.")
    else:
        for folder_url in meeting_folders:
            print(f"\nAttempting to process folder: {folder_url}")
            docs_url = urljoin(folder_url + '/', 'Docs/')
            excel_path = find_excel_in_docs(docs_url)

            if excel_path:
                print(f"Successfully downloaded {excel_path}")
                approved_crs = filter_approved_crs(excel_path, SPEC_NUMBER)
                
                if approved_crs:
                    print("\nSuccess! Found the following approved CRs:")
                    for rp, r4 in approved_crs:
                        print(f"  RP: {rp}, R4: {r4}")
                else:
                    print("\nSuccess! File was processed, but no approved CRs were found for this spec.")
                
                # Stop the test after the first successful processing
                print("--- Test complete --- ")
                break # Exit the loop
            else:
                print(f"Skipping folder. No accessible Excel file found.")
        else: # This else belongs to the for loop, executed if the loop finishes without break
            print("Test finished: Went through all folders but could not find any valid Excel sheets to process.")
