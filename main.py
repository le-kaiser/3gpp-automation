import requests
from bs4 import BeautifulSoup
import pandas as pd
import zipfile
import os
from urllib.parse import urljoin
import docx
from datetime import datetime
import re

# --- Configuration ---
BASE_URL = "https://www.3gpp.org/ftp/tsg_ran/TSG_RAN"
SPEC_NUMBER = "38.101-1"
# Using a set for efficient lookups
CLAUSES_DATABASE = {'4.1', '5.3.2', '7.1a', '5.3.6'}
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

def search_docx_for_clauses(docx_path, clauses_db):
    """
    Searches a .docx file for all affected clauses and the summary of change.
    Returns a tuple: (list_of_found_clauses, str_summary_text).
    """
    try:
        doc = docx.Document(docx_path)
        all_text = []
        for p in doc.paragraphs:
            all_text.append(p.text)
        for table in doc.tables:
            for row in table.rows:
                for cell in row.cells:
                    all_text.append(cell.text)

        found_clauses = set() # Use a set to avoid duplicate clause numbers
        # First, find all matching clauses
        for i, text in enumerate(all_text):
            if 'clauses affected' in text.lower():
                search_area = text
                if i + 1 < len(all_text):
                    search_area += " " + all_text[i+1]
                
                potential_clauses = re.findall(r'[\d\w\.-]+\.[\d\w\.-]+', search_area)
                for clause in potential_clauses:
                    cleaned_clause = clause.strip('., ')
                    if cleaned_clause in clauses_db:
                        found_clauses.add(cleaned_clause)

        if not found_clauses:
            return ([], None) # Return empty list if no clauses found

        # If clauses were found, now search for the summary
        summary_text = "Summary not found."
        summary_started = False
        summary_parts = []
        seen_summary_parts = set()
        stop_headers = ['consequences if not approved', 'clauses affected', 'isolated impact analysis']
        summary_start_index = -1

        # Find the start of the summary section
        for i, text in enumerate(all_text):
            if 'summary of change' in text.lower():
                summary_start_index = i
                if ':' in text:
                    possible_summary = text.split(':', 1)[1].strip()
                    if possible_summary and possible_summary not in seen_summary_parts:
                        summary_parts.append(possible_summary)
                        seen_summary_parts.add(possible_summary)
                break
        
        # If summary header was found, collect text until the next stop header
        if summary_start_index != -1:
            for i in range(summary_start_index + 1, len(all_text)):
                text_content = all_text[i]
                text_lower = text_content.lower().strip()
                
                if any(header in text_lower for header in stop_headers):
                    break
                
                cleaned_text = text_content.strip()
                if cleaned_text and cleaned_text not in seen_summary_parts:
                    summary_parts.append(cleaned_text)
                    seen_summary_parts.add(cleaned_text)
            
            if summary_parts:
                summary_text = "\n".join(summary_parts)

        return (list(found_clauses), summary_text)

    except Exception as e:
        print(f"Error reading docx file {docx_path}: {e}")
        return ([], None)

def process_rp_archive(docs_url, rp_number, r4_doc_name, clauses_db):
    """
    Downloads the RP archive, extracts the R4 doc, and triggers the search.
    """
    if not rp_number or not isinstance(rp_number, str):
        print(f"Invalid rp_number: {rp_number}")
        return ([], None)

    zip_url = urljoin(docs_url, rp_number + '.zip')
    zip_local_path = os.path.join(TEMP_DIR, rp_number + '.zip')
    extracted_docx_path = None
    result = ([], None)

    try:
        # Download the zip file
        print(f"Downloading archive: {zip_url}")
        with requests.get(zip_url, stream=True) as r:
            r.raise_for_status()
            with open(zip_local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        # Process the downloaded zip file
        with zipfile.ZipFile(zip_local_path) as z:
            target_docx_name = r4_doc_name + '.docx'
            file_in_zip = None
            # Find a case-insensitive match for the docx file
            for name in z.namelist():
                if name.lower().endswith(target_docx_name.lower()):
                    file_in_zip = name
                    break
            
            if file_in_zip:
                print(f"Found {file_in_zip} in archive. Extracting...")
                extracted_docx_path = z.extract(file_in_zip, path=TEMP_DIR)
                result = search_docx_for_clauses(extracted_docx_path, clauses_db)
            else:
                print(f"Could not find {target_docx_name} in {zip_local_path}")

    except requests.exceptions.RequestException as e:
        print(f"Failed to download {zip_url}. Error: {e}")
    except zipfile.BadZipFile:
        print(f"Error: {zip_local_path} is not a valid zip file.")
    except Exception as e:
        print(f"An unexpected error occurred in process_rp_archive: {e}")
    finally:
        # Clean up extracted and downloaded files
        if extracted_docx_path and os.path.exists(extracted_docx_path):
            os.remove(extracted_docx_path)
        if os.path.exists(zip_local_path):
            os.remove(zip_local_path)
            
    return result

def specific_pair_test_and_save(folder_url, target_rp, target_r4):
    """
    Runs a focused test on a single pair and saves the result to Excel.
    """
    print(f"--- Starting Specific Pair Test & Save on: {folder_url} ---")
    print(f"Target RP: {target_rp}, Target R4: {target_r4}")
    
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)

    docs_url = urljoin(folder_url + '/', 'Docs/')
    excel_file_path = find_excel_in_docs(docs_url)

    if not excel_file_path:
        print("Test Failed: Could not find or download the Excel file.")
        return

    relevant_crs = filter_approved_crs(excel_file_path, SPEC_NUMBER)

    if not relevant_crs:
        print("Test Failed: No relevant CRs found in the Excel file.")
        return

    pair_to_test = None
    for rp, r4 in relevant_crs:
        if rp == target_rp and r4 == target_r4:
            pair_to_test = (rp, r4)
            break
    
    if not pair_to_test:
        print(f"Test Failed: Could not find the specific pair RP={target_rp}, R4={target_r4} in the list.")
        return

    rp_number, r4_doc_name = pair_to_test
    print(f"\n--- Testing LIVE archive processing for specific pair ---")
    print(f"Processing RP: {rp_number}, R4: {r4_doc_name}")
    
    found_clauses, summary_text = process_rp_archive(docs_url, rp_number, r4_doc_name, CLAUSES_DATABASE)
    
    if found_clauses:
        print(f"\nSuccess! Found matching clause(s) in {r4_doc_name} for RP {rp_number}.")
        # Create and save the final output file for this single result
        results = [{
            'Meeting_Folder': folder_url,
            'RP_Number': rp_number,
            'R4_Document': r4_doc_name,
            'Matching_Clauses': ", ".join(found_clauses),
            'Summary_of_Change': summary_text
        }]
        print(f"\nSaving test result to {OUTPUT_FILE}")
        results_df = pd.DataFrame(results)
        results_df.to_excel(OUTPUT_FILE, index=False)
        print(f"Successfully saved test result to {os.path.abspath(OUTPUT_FILE)}")
    else:
        print(f"\nTest complete. No matching clauses found in the specified document.")

if __name__ == "__main__":
    # Target the specific folder and pair for the test
    test_folder_url = "https://www.3gpp.org/ftp/tsg_ran/TSG_RAN/TSGR_109/"
    test_rp = "RP-252378"
    test_r4 = "R4-2511059"
    specific_pair_test_and_save(test_folder_url, test_rp, test_r4)
