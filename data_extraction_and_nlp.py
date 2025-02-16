import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
from bs4 import BeautifulSoup
import re
import glob
import os
import argparse
from functools import lru_cache
from tqdm import tqdm
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.tag import pos_tag
from nltk.corpus import cmudict
nltk.download('cmudict')  # Download the CMU Pronouncing Dictionary
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger_eng')


class data_extraction_and_nlp:
    def __init__(self, input_file, output_file, stop_words_directory,masterDictionary_directory, extracted_files_directory, filter_extracted_directory, default_chrome_profile_path=None):
        self.input_file = input_file
        self.output_file = output_file
        self.stop_words_directory = stop_words_directory
        self.masterDictionary_directory = masterDictionary_directory
        self.extracted_files_directory = extracted_files_directory
        self.filter_extracted_directory = filter_extracted_directory
        self.default_chrome_profile_path = default_chrome_profile_path
        self.url_df = None  
        self.text_df = None
        self.stop_words = set()
        self.stop_words_files = list()
        self.extracted_file_list = list()

    def create_check_folders(self):
        """Checking input folder and creating intermediate folders if they don't exist."""
        print("Checking input folders...")
        if not(os.path.isdir(self.stop_words_directory)):
            print(f"{self.stop_words_directory}folder doesn't exisit")
        if not(os.path.isdir(self.masterDictionary_directory)):
             print(f"{self.stop_words_directory}folder doesn't exisit")
        print("Creating intermediate folders...")
        if (os.path.isdir(self.extracted_files_directory)):
            print(f"{self.extracted_files_directory} folder already exisit files might be overwritten")
        else:
            os.makedirs(self.extracted_files_directory)
            print(f"Folders created: {self.extracted_files_directory}")
        if (os.path.isdir(self.filter_extracted_directory)):
            print(f"{self.filter_extracted_directory} folder already exisit files might be overwritten")
        else:
            os.makedirs(self.filter_extracted_directory)
            print(f"Folders created: {self.filter_extracted_directory}")

    def load_data(self):
        """Load the data from the input Excel file using pandas."""
        print(f"Loading data from {self.input_file}...")
        self.url_df =pd.read_excel(r"G:\20211030 Test Assignment\Input.xlsx",header=0)
        print("Data loaded successfully.")

    def scroll_to_end(self,driver):
        """Function to scroll to the end of web page."""
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            # Calculate new scroll height and compare with last height
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def get_default_chrome_driver(self):
        """Gets the default Chrome WebDriver instance."""
        try:
            service = ChromeService(executable_path=ChromeDriverManager().install())
            options = webdriver.ChromeOptions()
            options.add_argument(self.default_chrome_profile_path) 
            # Replace with your actual default Chrome profile path
            options.add_argument("--headless")

            driver = webdriver.Chrome(service=service, options=options)
            return driver
        except Exception as e:
            print(f"Error initializing driver: {e}")
            raise Exception(f"Error initializing driver: {e}")

    def fetch_and_format_article(self,url):
        """Extracting text from url using selinum chrome driver and bs4"""
        try:
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
        except:
            if(self.default_chrome_profile_path == None):
                raise Exception("Required default Chrome profile path as chrome driver needs to be downloaded")
            driver = self.get_default_chrome_driver()
        try:
            driver.get(url)
            # Scroll to the end of the page
            self.scroll_to_end(driver)
            page_html = driver.page_source
        finally:
            driver.quit()

        soup = BeautifulSoup(page_html, 'html.parser')
        start_element = soup.find('header')
        stop_element = soup.find('footer')

        # Checks
        if not start_element:
            raise ValueError(f"Error: Start element (header) not found in the page -{url}.")
        if not stop_element:
            raise ValueError(f"Error: Stop element (footer) not found in the page. -{url}")
        
        # Extract content
        content_to_process = []
        for sibling in start_element.find_all_next():
            if sibling == stop_element:
                break
            content_to_process.append(sibling)

        # unwanted headers
        unwanted_headers = ["Contact Details", "Project Snapshots", "Project website url" , "Summarize"]

        # Extract and format text content
        def extract_text_with_structure(elements):
            lines = []
            skip_section = False
            for element in elements:
                if element.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                    if element.get_text(strip=True) in unwanted_headers:
                        skip_section = True
                    else:
                        skip_section = False 

                if skip_section:
                    continue

                if element.name in ['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'blockquote']:
                    text = element.get_text(separator=' ', strip=True)
                    if text:
                        lines.append(f"\n{text}")
                elif element.name is None and element.string and element.string.strip():
                    lines.append(element.string.strip())
            return '\n'.join(lines)

        formatted_text = extract_text_with_structure(content_to_process)
        return formatted_text

    def extracting_files(self):
        """Function to extract HTML Files"""
        if os.path.isdir(self.extracted_files_directory):
            txt_files = glob.glob(os.path.join(self.extracted_files_directory, "*.txt"))
            if txt_files:
                print("Extracted text files already present")
                return
        for index, row in tqdm(self.url_df.iterrows(),total=self.url_df.shape[0],desc="Extracting URL"):
            file_name = os.path.join(self.extracted_files_directory, row.iloc[0] + ".txt")
            url = row.iloc[1]
            formatted_text = self.fetch_and_format_article(url)
            with open(file=file_name, mode='w+', encoding="utf-8") as temp:
                temp.write(formatted_text)
        print("Extracted HTML Files successfully.")

    def stop_words_files_list(self):
        f"""Creates a list of all files in {self.stop_words_directory} """
        glob_stop_words_directory = r""+self.stop_words_directory+"/*"
        for txt_files in glob.glob(glob_stop_words_directory):
            self.stop_words_files.append(txt_files)
        print("Stop words List created successfully.")

    def extracted_files_list(self):
        f"""Creates a list of all files in {self.extracted_files_directory} """
        glob_extracted_text_files_director = r""+self.extracted_files_directory+"/*"
        for extracted_file in glob.glob(glob_extracted_text_files_director):
            self.extracted_file_list.append(extracted_file)
        print("Extracted files List created successfully.")

    def remove_stop_words(self, batch_size=100):
        f"""Removes all stop_words given in the {self.stop_words_directory} Folder"""
        stop_word_files = self.stop_words_files
        extracted_text_files = self.extracted_file_list
        # Stop words from all files
        stop_words_set = set()
        for txt_file in stop_word_files:
            stop_words = pd.read_csv(
                filepath_or_buffer=txt_file, header=None, delimiter="|", encoding="ISO-8859-1"
            )[0].str.strip().str.lower().tolist()
            stop_words_set.update(stop_words)

        if os.path.isdir(self.filter_extracted_directory):
            txt_files = glob.glob(os.path.join(self.filter_extracted_directory, "*.txt"))
            if txt_files:
                print("Filtered text files already present")
                return
        # Extracted text file
        for extracted_file in extracted_text_files:
            # Read lines from the input file
            with open(extracted_file, "r", encoding="utf-8") as f:
                lines = f.readlines()

            # Create the output file path
            file_name = os.path.basename(extracted_file)
            output_file_path = os.path.join(self.filter_extracted_directory, file_name)

            # Write filtered text in batches
            with open(output_file_path, "w", encoding="utf-8") as f:
                for i in range(0, len(lines), batch_size):
                    batch = lines[i:i+batch_size]
                    for line in batch:
                        # Clean the line
                        cleaned_line = re.sub(
                            r"(@[A-Za-z_]+)|([^A-Za-z0-9\s])|(\w+:\/\/\S+)|\brt\b|http.+?", 
                            " ", 
                            line
                        )
                        # Filter stop words
                        words = cleaned_line.lower().split()
                        filtered_words = [word for word in words if word not in stop_words_set]
                        if filtered_words:
                            f.write(" ".join(filtered_words) + "\n")

    def create_df_from_files(self,batch_size=10):
        f"""Creates a Dataframe with {self.input_file} and filtered text"""
        filtered_text_files=list()
        for file_path in glob.glob(pathname=os.path.join(self.filter_extracted_directory, '**/*.txt'), recursive=True):
            filtered_text_files.append(file_path)
        data = list()
        for i in range(0, len(filtered_text_files), batch_size):
            batch = filtered_text_files[i:i+batch_size]
            for file_path in batch:
                # Extract filename
                filename = os.path.splitext(os.path.basename(file_path))[0] 
                # Read the file content
                with open(file_path, 'r', encoding='utf-8') as f:
                    text = f.read()
                data.append([filename, text])

        # Create the DataFrame
        temp_df = pd.DataFrame(data, columns=['Filename', 'Text'])
        self.url_df=pd.read_excel(self.input_file,header=0)
        self.text_df = pd.merge(self.url_df,temp_df,left_on="URL_ID",right_on="Filename",how="left")
        self.text_df.drop(columns='Filename',axis=1,inplace=True)

    def remove_new_line_char(self,string):
        """Removes all newline characters"""
        return string.replace("\n", " ")

    def positive_score(self,string):
        f"""Count word if found in the Positive Dictionary in {self.masterDictionary_directory} and then adding up all the value"""
        positive_words = pd.read_csv(filepath_or_buffer=os.path.join(self.masterDictionary_directory,"positive-words.txt"),header=None, delimiter="|", encoding="ISO-8859-1")[0].str.strip().str.lower().tolist()
        positive_words = set(positive_words)
        positive_count=0
        for word in string.split(): 
            if word in positive_words:
                positive_count += 1
        return positive_count

    def negative_score(self,string):
        f"""Count word if found in the Negative Dictionary in {self.masterDictionary_directory} and then adding up all the value"""
        negative_words = pd.read_csv(filepath_or_buffer=os.path.join(self.masterDictionary_directory,"negative-words.txt"),header=None, delimiter="|", encoding="ISO-8859-1")[0].str.strip().str.lower().tolist()
        negative_words = set(negative_words)
        negative_count=0
        for word in string.split(): 
            if word in negative_words:
                negative_count += 1
        return negative_count

    def polarity_score(self,row):
        """Polarity score determines if a given text is positive or negative"""
        positive_score = row['POSITIVE SCORE']
        negative_score = row['NEGATIVE SCORE']
        denominator = positive_score + negative_score + 0.000001
        polarity_score = (positive_score - negative_score) / denominator
        return polarity_score

    def subjectivity_score(self,row):
        """Subjectivity Score Determines if a given text is objective or subjective"""
        positive_score = row['POSITIVE SCORE']
        negative_score = row['NEGATIVE SCORE']
        total_words_after_cleaning = len(row['Text_cleaned'].split())
        subjectivity_score = (positive_score - negative_score) / (total_words_after_cleaning + 0.000001)
        return subjectivity_score

    def avg_sentence_length(self,row):
        """Function to find Average Sentence Length"""
        words_per_row = len(row['Text_cleaned'].split())
        sentence_per_row = len(row['Text'].split("\n"))
        avg_sentence_length = words_per_row / sentence_per_row
        return avg_sentence_length

    def sylco(self,word) :
        word = word.lower()

        exception_add = ['serious','crucial']
        exception_del = ['fortunately','unfortunately']
        co_one = ['cool','coach','coat','coal','count','coin','coarse','coup','coif','cook','coign','coiffe','coof','court']
        co_two = ['coapt','coed','coinci']
        pre_one = ['preach']
        syls = 0 #added syllable number
        disc = 0 #discarded syllable number
        if len(word) <= 3 :
            syls = 1
            return syls
        #2) if doesn't end with "ted" or "tes" or "ses" or "ied" or "ies", discard "es" and "ed" at the end.
        # if it has only 1 vowel or 1 set of consecutive vowels, discard. (like "speed", "fled" etc.)
        if word[-2:] == "es" or word[-2:] == "ed" :
            doubleAndtripple_1 = len(re.findall(r'[eaoui][eaoui]',word))
            if doubleAndtripple_1 > 1 or len(re.findall(r'[eaoui][^eaoui]',word)) > 1 :
                if word[-3:] == "ted" or word[-3:] == "tes" or word[-3:] == "ses" or word[-3:] == "ied" or word[-3:] == "ies" :
                    pass
                else :
                    disc+=1
        #3) discard trailing "e", except where ending is "le"  
        le_except = ['whole','mobile','pole','male','female','hale','pale','tale','sale','aisle','whale','while']
        if word[-1:] == "e" :
            if word[-2:] == "le" and word not in le_except :
                pass
            else :
                disc+=1
        #4) check if consecutive vowels exists, triplets or pairs, count them as one.
        doubleAndtripple = len(re.findall(r'[eaoui][eaoui]',word))
        tripple = len(re.findall(r'[eaoui][eaoui][eaoui]',word))
        disc+=doubleAndtripple + tripple
        #5) count remaining vowels in word.
        numVowels = len(re.findall(r'[eaoui]',word))
        #6) add one if starts with "mc"
        if word[:2] == "mc" :
            syls+=1
        #7) add one if ends with "y" but is not surrouned by vowel
        if word[-1:] == "y" and word[-2] not in "aeoui" :
            syls +=1
        #8) add one if "y" is surrounded by non-vowels and is not in the last word.
        for i,j in enumerate(word) :
            if j == "y" :
                if (i != 0) and (i != len(word)-1) :
                    if word[i-1] not in "aeoui" and word[i+1] not in "aeoui" :
                        syls+=1
        #9) if starts with "tri-" or "bi-" and is followed by a vowel, add one.
        if word[:3] == "tri" and word[3] in "aeoui" :
            syls+=1
        if word[:2] == "bi" and word[2] in "aeoui" :
            syls+=1
        #10) if ends with "-ian", should be counted as two syllables, except for "-tian" and "-cian"
        if word[-3:] == "ian" : 
        #and (word[-4:] != "cian" or word[-4:] != "tian") :
            if word[-4:] == "cian" or word[-4:] == "tian" :
                pass
            else :
                syls+=1
        #11) if starts with "co-" and is followed by a vowel, check if exists in the double syllable dictionary, if not, check if in single dictionary and act accordingly.
        if word[:2] == "co" and word[2] in 'eaoui' :

            if word[:4] in co_two or word[:5] in co_two or word[:6] in co_two :
                syls+=1
            elif word[:4] in co_one or word[:5] in co_one or word[:6] in co_one :
                pass
            else :
                syls+=1
        #12) if starts with "pre-" and is followed by a vowel, check if exists in the double syllable dictionary, if not, check if in single dictionary and act accordingly.
        if word[:3] == "pre" and word[3] in 'eaoui' :
            if word[:6] in pre_one :
                pass
            else :
                syls+=1
        #13) check for "-n't" and cross match with dictionary to add syllable.
        negative = ["doesn't", "isn't", "shouldn't", "couldn't","wouldn't"]
        if word[-3:] == "n't" :
            if word in negative :
                syls+=1
            else :
                pass   
        #14) Handling the exceptional words.
        if word in exception_del :
            disc+=1

        if word in exception_add :
            syls+=1     
        # calculate the output
        return numVowels - disc + syls

    def complex_word_count(self,text):
        """Words in the text that contain more than two syllables"""
        pronouncing_dict = cmudict.dict()
        # LRU Cache
        @lru_cache(maxsize=100000)
        def get_syllables(word):
            try:
                pronunciation = pronouncing_dict[word][0]
                syllables = [phoneme for phoneme in pronunciation if phoneme[-1].isdigit()]
                return len(syllables)
            except KeyError:
                return self.sylco(word) 

        words = nltk.word_tokenize(text.lower())
        complex_word_count = 0

        for word in words:
            if word.isalpha(): 
                syllable_count = get_syllables(word)
                if syllable_count > 2:
                    complex_word_count += 1
        return complex_word_count

    def percentage_of_complex_words(self,row):
        """Function to find Percentage of Complex word"""
        total_words_after_cleaning = len(row['Text_cleaned'].split()) 
        percentage_of_complex_words = row['COMPLEX WORD COUNT'] / total_words_after_cleaning
        return percentage_of_complex_words

    def avg_number_of_words_per_sentence(self,row):
        """Function to caluclate average Number of Words Per Sentence"""
        words_per_row = len(row['Text_cleaned'].split())
        sentence_per_row = len(row['Text'].split("\n"))
        avg_number_of_words_per_sentence = words_per_row/sentence_per_row
        return avg_number_of_words_per_sentence

    def word_count(self,row):
        """Function to ccount the total cleaned words present"""
        word_count = len(row['Text_cleaned'].split())
        return word_count

    def syllable_per_word(self,row):
        """Function to caluclate number of Syllables in each word"""
        cmu_dict = cmudict.dict()
        #lru cache
        @lru_cache(maxsize=100000)
        def syllable_count(word):
            try:
                # Check CMU dictionary for syllable count
                pronunciations = cmu_dict[word.lower()]
                pronunciation = pronunciations[0]
                return len([x for x in pronunciation if x[-1].isdigit()])
            except KeyError:
                return self.sylco(word)

        # Precompute unique word syllable counts
        unique_words = set(word for text in row['Text_cleaned'] for word in text.split())
        syllable_cache = {word: syllable_count(word) for word in unique_words}

        words = row['Text_cleaned'].split()
        return {word: syllable_cache.get(word, self.sylco(word)) for word in words}

    def personal_pronouns(self):
        """Function to calculate Personal Pronouns"""
        def count_pronouns(text_list):
            pronouns = ["i", "we", "my", "ours", "us"]
            total_count = 0

            for text in text_list:
                for match in re.finditer(r"\b(i|we|my|ours|us)\b", text.lower()):
                    pronoun = match.group(1)
                    if pronoun == "us":
                        # Check preceding and following context
                        start_idx = match.start()
                        preceding_text = text[:start_idx].strip().lower()
                        following_text = text[match.end():].strip().lower()

                        # If preceded by "the", "a", or "an" and not standalone then skip
                        if preceding_text.endswith(("the", "a", "an")) and (
                            following_text.startswith((" ", ",", ".", ";", ":"))
                            or following_text == ""):
                            continue
                    total_count += 1

            return total_count
  
        data=list()
        for extracted_file in self.extracted_file_list:
            filename, _ = os.path.splitext(os.path.basename(extracted_file))
            with open(file= extracted_file,mode="r", encoding="utf-8") as f:
                lines = f.readlines()
            pronoun_counts = count_pronouns(lines)
            data.append([filename, pronoun_counts])
        temp_df = pd.DataFrame(data, columns=['Filename', 'PERSONAL PRONOUNS'])
        self.text_df = pd.merge(self.text_df,temp_df,left_on="URL_ID",right_on="Filename",how="left")
        self.text_df.drop(columns='Filename',axis=1,inplace=True)

        return self.text_df

    def avg_word_length(self,row):
        """Function to caluclate average Word Length"""
        total_number_of_char=len([char for char in row['Text_cleaned'] if char.isalnum()])
        total_words_after_cleaning = len(row['Text_cleaned'].split())
        avg_word_length = total_number_of_char/total_words_after_cleaning
        return avg_word_length 

    def clean_up(self):
        f"""Drops all unusefull columns and reorders columns as per {self.output_file}"""
        order=["URL_ID", "URL", "POSITIVE SCORE", "NEGATIVE SCORE", "POLARITY SCORE", "SUBJECTIVITY SCORE",
            "AVG SENTENCE LENGTH", "PERCENTAGE OF COMPLEX WORDS", "FOG INDEX", "AVG NUMBER OF WORDS PER SENTENCE",
            "COMPLEX WORD COUNT", "WORD COUNT", "SYLLABLE PER WORD", "PERSONAL PRONOUNS", "AVG WORD LENGTH"]										
        self.text_df.drop(columns=["Text","Text_cleaned"])
        output_file_df  = self.text_df[order]
        output_file_df.to_excel(self.output_file,header=True,index=False)

    def process(self):
        """Process the entire workflow."""
        self.create_check_folders()
        self.load_data()
        self.extracting_files()
        self.stop_words_files_list()
        self.extracted_files_list()
        self.remove_stop_words()
        self.create_df_from_files()
        tqdm.pandas(desc="Cleaning Text")
        self.text_df['Text_cleaned'] = self.text_df['Text'].progress_apply(self.remove_new_line_char)
        
        tqdm.pandas(desc="Calculating Positive Score")
        self.text_df['POSITIVE SCORE'] = self.text_df['Text_cleaned'].progress_apply(self.positive_score)
        
        tqdm.pandas(desc="Calculating Negative Score")
        self.text_df['NEGATIVE SCORE'] = self.text_df['Text_cleaned'].progress_apply(self.negative_score)
        
        tqdm.pandas(desc="Calculating Polarity Score")
        self.text_df['POLARITY SCORE'] = self.text_df.progress_apply(self.polarity_score, axis=1)
        
        tqdm.pandas(desc="Calculating Subjectivity Score")
        self.text_df['SUBJECTIVITY SCORE'] = self.text_df.progress_apply(self.subjectivity_score, axis=1)
        
        tqdm.pandas(desc="Calculating Average Sentence Length")
        self.text_df['AVG SENTENCE LENGTH'] = self.text_df.progress_apply(self.avg_sentence_length, axis=1)

        tqdm.pandas(desc="Counting Complex Words")
        self.text_df['COMPLEX WORD COUNT'] = self.text_df['Text_cleaned'].progress_apply(self.complex_word_count)

        tqdm.pandas(desc="Calculating Percentage of Complex Words")
        self.text_df['PERCENTAGE OF COMPLEX WORDS'] = self.text_df.progress_apply(self.percentage_of_complex_words, axis=1)

        self.text_df['FOG INDEX'] = 0.4 * self.text_df['AVG SENTENCE LENGTH'] + self.text_df['PERCENTAGE OF COMPLEX WORDS']
        
        tqdm.pandas(desc="Calculating Average Number of Words Per Sentence")
        self.text_df['AVG NUMBER OF WORDS PER SENTENCE'] = self.text_df.progress_apply(self.avg_sentence_length, axis=1)

        tqdm.pandas(desc="Counting Words")
        self.text_df['WORD COUNT'] = self.text_df.progress_apply(self.word_count, axis=1)

        tqdm.pandas(desc="Counting Syllables Per Word")
        self.text_df['SYLLABLE PER WORD'] = self.text_df.progress_apply(self.syllable_per_word, axis=1)

        self.personal_pronouns()

        tqdm.pandas(desc="Calculating Average Word Length")
        self.text_df["AVG WORD LENGTH"] = self.text_df.progress_apply(self.avg_word_length, axis=1)
        self.clean_up()

        print("inished the workflow")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Data Extraction and NLP Processing")
    parser.add_argument("input_file", help="Input Excel file")
    parser.add_argument("output_file", help="Output Excel file")
    parser.add_argument("stop_words_directory", help="Directory containing stop word files")
    parser.add_argument("masterDictionary_directory", help="Master dictionary containing positive and negative to apply during processing")
    parser.add_argument("extracted_files_directory", help="Directory containing extracted text files")
    parser.add_argument("filter_extracted_directory", help="Directory to save filtered text files")
    parser.add_argument("--default_chrome_profile_path", 
                        help="default Chrome profile path (Required if selinum chrome driver is not installed)(optional)", 
                        default=None)

    args = parser.parse_args()

    # Initialize the data extraction and NLP processor with the passed arguments
    processor = data_extraction_and_nlp(
        input_file=args.input_file,
        output_file=args.output_file,
        stop_words_directory=args.stop_words_directory,
        masterDictionary_directory=args.masterDictionary_directory,
        extracted_files_directory=args.extracted_files_directory,
        filter_extracted_directory=args.filter_extracted_directory,
        default_chrome_profile_path=args.default_chrome_profile_path
    )

    # Run the processing
    processor.process()

if __name__ == "__main__":
    main()