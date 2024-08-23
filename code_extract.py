from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.service import Service
import csv
import os
import time

# Initialize WebDriver (replace with your WebDriver path)
driver_path = r'C:\Users\hanum\OneDrive\Desktop\view\basic\chromedriver-win64\chromedriver.exe'
service = Service(executable_path=driver_path)
driver = webdriver.Chrome(service=service)

def reset_form():
    # Function to reset the form (implement as needed)
    print("Resetting the form...")
    # Example: driver.find_element(By.XPATH, 'xpath_to_reset_button').click()

def re_select_options():
    # Function to re-select options in the form
    print("Re-selecting options...")
    # Example:
    # select_field_1 = Select(driver.find_element(By.XPATH, '//*[@id="est_code"]'))
    # select_field_1.select_by_visible_text('Kukatpally, ADJ Court Complex')
    # select_field_2 = Select(driver.find_element(By.XPATH, '//*[@id="court"]'))
    # select_field_2.select_by_visible_text('3 Smt.Girija Tirandasu - I Addl. Junior Civil Judge-cum-XII Addl.Metropolitan Magistrate,Medchal-Malkajgiri Dist. at Kukatpally')

def extract_and_append_data():
    combined_data = []
    while True:
        driver.get('https://medchalmalkajgiri.dcourts.gov.in/cause-list-%e2%81%84-daily-board/')
        print("Website opened successfully.")

        # Wait until the elements are present
        wait = WebDriverWait(driver, 10)

        try:
            # Select an option from the first select field
            select_field_1 = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="est_code"]')))
            select1 = Select(select_field_1)
            select1.select_by_visible_text('Kukatpally, ADJ Court Complex')

            # Select an option from the second select field
            select_field_2 = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="court"]')))
            select2 = Select(select_field_2)
            select2.select_by_visible_text('3 Smt.Girija Tirandasu - I Addl. Junior Civil Judge-cum-XII Addl.Metropolitan Magistrate,Medchal-Malkajgiri Dist. at Kukatpally')

            # Click the date picker
            date_picker_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="myDatepicker"]/div[1]/button')))
            date_picker_button.click()

            # Pause for manual date selection and captcha entry
            input("Select the date manually, enter the captcha, and then press Enter...")

            # Extract the selected date from the date input field
            selected_date_element = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="date"]')))
            selected_date = selected_date_element.get_attribute('value')
            print(selected_date)

            # Click the search button
            search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="ecourt-services-cause-list-cause-list"]/div[8]/div[2]/input[1]')))
            search_button.click()
            print("Search button pressed")

            # Check for CAPTCHA error message
            try:
                captcha_error_message = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="cnrResults"]/div')))
                if "The captcha code entered was incorrect" in captcha_error_message.text:
                    print("CAPTCHA is incorrect. Refreshing form...")
                    time.sleep(5)
                    reset_form()
                    re_select_options()
                    continue  # Restart the loop for new date entry
            except Exception as e:
                print(f"Error checking CAPTCHA error message: {e}")

            # Check for "No Records Found" message
            try:
                no_records_message = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="cnrResults"]/div')))
                if "No records found" in no_records_message.text:
                    print("No records found on this date.")
                    time.sleep(2)
                    reset_form()
                    re_select_options()
                    continue  # Restart the loop for new date entry
            except Exception as e:
                print(f"Error checking 'No records found' message: {e}")

            # Wait for the table to be present
            table_body = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="cnrResults"]/div[2]/table/tbody')))
            rows = table_body.find_elements(By.TAG_NAME, "tr")

            # Extract data from the main table
            main_table_data = []
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                row_data = [col.text for col in cols]
                main_table_data.append(row_data)

            for row_index, row_data in enumerate(main_table_data):
                case_type = row_data[0] if len(row_data) > 0 else ''
                case_type_parts = case_type.split(' ')
                last_two_parts = case_type_parts[-2:] if len(case_type_parts) >= 2 else [''] * 2

                try:
                    print(f"Attempting to click the link in row {row_index}...")
                    time.sleep(2)
                    cols = rows[row_index].find_elements(By.TAG_NAME, "td")
                    span_tag = cols[1].find_element(By.TAG_NAME, 'span')
                    link = span_tag.find_element(By.TAG_NAME, 'a')
                    driver.execute_script("arguments[0].click();", link)

                    time.sleep(5)

                    # Extract data from additional tables
                    new_table_1_body = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="cnrResultsDetails"]/div[2]/table/tbody')))
                    new_table_1_rows = new_table_1_body.find_elements(By.TAG_NAME, "tr")
                    new_table_1_data = [new_table_1_row.find_elements(By.TAG_NAME, "td") for new_table_1_row in new_table_1_rows]

                    new_table_2_body = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="cnrResultsDetails"]/div[3]/table/tbody')))
                    new_table_2_rows = new_table_2_body.find_elements(By.TAG_NAME, "tr")
                    new_table_2_data = [new_table_2_row.find_elements(By.TAG_NAME, "td") for new_table_2_row in new_table_2_rows]

                    new_table_3_body = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="cnrResultsDetails"]/div[5]/table/tbody')))
                    new_table_3_rows = new_table_3_body.find_elements(By.TAG_NAME, "tr")
                    new_table_3_data = [new_table_3_row.find_elements(By.TAG_NAME, "td") for new_table_3_row in new_table_3_rows]

                    new_row_data = [cell.text for cell in new_table_1_data[0]] if new_table_1_data else [''] * len(new_table_1_rows[0].find_elements(By.TAG_NAME, "td"))
                    additional_row_data = [cell.text for cell in new_table_2_data[0]] if new_table_2_data else [''] * len(new_table_2_rows[0].find_elements(By.TAG_NAME, "td"))
                    third_table_data = [cell.text for cell in new_table_3_data[0]] if new_table_3_data else [''] * len(new_table_3_rows[0].find_elements(By.TAG_NAME, "td"))

                    additional_row_data.append(selected_date)

                    combined_row = row_data + new_row_data + additional_row_data + third_table_data + last_two_parts
                    combined_data.append(combined_row)

                    print("Attempting to click the back button...")
                    back_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="row-content"]/div[2]/div/div/div/div[2]/button[1]')))
                    back_button.click()
                    time.sleep(3)

                    # Wait for the table to be present again after navigation
                    wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="cnrResults"]/div[2]/table/tbody')))
                    table_body = driver.find_element(By.XPATH, '//*[@id="cnrResults"]/div[2]/table/tbody')
                    rows = table_body.find_elements(By.TAG_NAME, "tr")

                except Exception as e:
                    print(f"An error occurred while processing row {row_index}: {e}")
                    driver.save_screenshot(f'link_error_screenshot_{row_index}.png')
                    wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="cnrResults"]/div[2]/table/tbody')))
                    table_body = driver.find_element(By.XPATH, '//*[@id="cnrResults"]/div[2]/table/tbody')
                    rows = table_body.find_elements(By.TAG_NAME, "tr")

            new_csv_file_path = r'C:\Users\hanum\OneDrive\Desktop\view\cards\new_court_data.csv'
            file_exists = os.path.isfile(new_csv_file_path)
            try:
                with open(new_csv_file_path, mode='a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    if not file_exists:
                        headers = ["Serial Number", "Case Type", "Party Name", "Advocate", "Selected Date", "Case Type", "Filing Number", "Filing Date", "Registration Date", "Registration Number", "CNR Number", "First Hearing Date", "Next Hearing Date", "Case Status", "Stage of Case", "Court Number and Judge", "Act", "Selected Date", "Filing ID"]
                        writer.writerow(headers)
                    writer.writerows(combined_data)
                    print(f"Data appended to {new_csv_file_path}")
            except Exception as e:
                print(f"Failed to write data to CSV: {e}")

            # Optionally break the loop after completing the data extraction for one round
            # break

        except Exception as e:
            print(f"An error occurred: {e}")
            driver.save_screenshot('error_screenshot.png')
            reset_form()
            re_select_options()

if __name__ == "__main__":
    extract_and_append_data()
    driver.quit()
