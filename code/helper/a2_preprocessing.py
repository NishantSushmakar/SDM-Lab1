import json
import os
import pandas as pd 
import random
from faker import Faker
from datetime import datetime
import re
import uuid

json_dir = './paper_data'

domain = {
    "Computer Science":["databases","transformer","encryption","mining", "data management", "indexing", "data modeling", "big data", 
          "data processing", "data storage", "data querying"],
    "Medicine":["diagnosis","health","disease","Genomic"],
    "Biology": ["genome","Epigenomic","phylogenetics","receptor"],
    "Chemistry":["synthesis","Bioinorganic","Molecular","Chromatographic"],
    "Mathematics":["curves","probability","Harmonic","hyperbolic"]
}

ORDINAL_WORDS = {
    'first': 1, 'second': 2, 'third': 3, 'fourth': 4, 'fifth': 5, 'sixth': 6,
    'seventh': 7, 'eighth': 8, 'ninth': 9, 'tenth': 10, 'eleventh': 11,
    'twelfth': 12, 'thirteenth': 13, 'fourteenth': 14, 'fifteenth': 15,
    'sixteenth': 16, 'seventeenth': 17, 'eighteenth': 18, 'nineteenth': 19,
    'twentieth': 20, 'twentyfirst': 21, 'twentysecond': 22, 'twentythird': 23,
    'twentyfourth': 24, 'twentyfifth': 25, 'twentysixth': 26, 'twentyseventh': 27,
    'twentyeighth': 28, 'twentyninth': 29, 'thirtieth': 30
}

peer_reviews = [
    "The study addresses an important gap, but the small sample size limits generalizability. Recommend major revisions.",
    "Methodology is robust, but the discussion lacks recent literature. Include more citations before acceptance.",
    "Theoretical framework is strong, but empirical validation is missing. Add case studies for clarity.",
    "Results are compelling, but statistical analysis requires deeper justification. Revise and resubmit.",
    "Innovative topic, but the writing is unclear in sections 2 and 4. Improve flow and terminology.",
    "Data presentation is excellent, but conclusions overreach the findings. Tone down claims.",
    "Strong interdisciplinary approach, but figures lack labels. Redesign visuals for better readability.",
    "Well-structured argument, but lacks engagement with counterarguments. Expand the literature review.",
    "Practical implications are significant, but ethical considerations are absent. Address in revision.",
    "Original contribution to the field, but the abstract needs simplification. Clarify key takeaways."
]
review_decisions = ['accepted', 'rejected']

fake = Faker()

def assign_reviewer_research_paper(paper_lst,h_index_threshold = 5):
    

    ## Create the domain of the authors 
    domain_author = {}

    for paper in paper_lst:
        if paper['fieldsOfStudy'] != None:
            for domain in paper['fieldsOfStudy']:
                for author in paper['authors']:
                    if domain not in domain_author.keys():
                        domain_author[domain] = [author['authorId']]
                    else:
                        domain_author[domain].append(author['authorId'])

    print("Author Domain Created")
    ## Load Author

    with open(f'./paper_data/authors_details_new.json','r') as f:
        author_details = json.load(f)

    print("Author Details Loaded")

    h_index_lst = []

    for author in author_details:
        if author['hIndex'] != None:
            h_index_lst.append(author['hIndex'])
    df = pd.DataFrame(h_index_lst,columns=['h_index'])

    author_h_index = {}

    for author in author_details:
        if author['hIndex'] != None:
            author_h_index[author['authorId']] = author['hIndex']
        else:
            author_h_index[author['authorId']] = 0


    paper_reviewers = {}
    paper_with_no_reviewers = []

    for paper in paper_lst:

        paper_author = set([author['authorId'] for author in paper['authors']])

        
        if paper['fieldsOfStudy'] != None:
            selected_author = []
            for domain in paper['fieldsOfStudy'] :
                selected_author.extend(domain_author[domain])
            
            selected_author = list(set(selected_author))

            selected_author = set([ids for ids in selected_author if ids != None and ids in author_h_index.keys() and author_h_index[ids]>=h_index_threshold])

            choosen_authors = list(selected_author.difference(paper_author))


            if len(choosen_authors)>=3:

                num_ids = random.choice([2,3])
                selected_ids = random.sample(choosen_authors,num_ids)

                paper_reviewers[paper['paperId']] = selected_ids
        
            elif len(choosen_authors)>0:
                paper_reviewers[paper['paperId']] = selected_ids
            else:
                paper_with_no_reviewers.append(paper['paperId'])
        else:
            paper_with_no_reviewers.append(paper['paperId'])

    with open("./paper_data/paper_reviewers.json","w") as f:
        json.dump(paper_reviewers, f)

    print("Paper Reviewers Assigned and Data Stored!!")



def assign_keywords_research_paper(paper_lst):

    alex_info_lst=[]
    with open(f'./paper_data/alex_info.json','r') as f:
        alex_info_lst = json.load(f)

    alex_info_dict = {}

    for paper in alex_info_lst:

        if 'keywords' in paper.keys():
            alex_info_dict[paper['paperId']] =  [x['display_name'] for x in paper['keywords'] if x['display_name']!='Plain Text']

    
    paper_keywords = {}

    for paper in paper_lst:
        initial_keywords = {kw.lower(): kw for kw in alex_info_dict.get(paper['paperId'], [])}
        
        if paper['fieldsOfStudy'] is not None:
            for field in paper['fieldsOfStudy']:
                if field in domain:
                    for word in domain[field]:
                        if word.lower() in paper['title'].lower():
                            initial_keywords[word.lower()] = word  # Overwrites with last seen case
        
        paper_keywords[paper['paperId']] = list(initial_keywords.values())

    

    with open("./paper_data/paper_keywords.json",'w') as f:
        json.dump(paper_keywords,f)

    print("Keywords Assigned Successfully!!!")

def generate_venue():
    city = fake.city()
    country = fake.country()
    institution = fake.random_element(elements=(
        "University of " + fake.city(),
        fake.company() + " Research Center",
        "International " + fake.word().title() + " Institute"
    ))
    return {
        "city": city,
        "country": country,
        "institution": institution
    }

def extract_edition(conference_name):
    pattern = re.compile(
        r"""
        (\b\d{4}\b)|                        # 4-digit year (group 1)
        (\d+)(?:st|nd|rd|th)\b|             # Numeric ordinal (group 2)
        Edition\s+(\d+)|                    # Explicit "Edition X" (group 3)
        ['’](\d{2})\b|                      # Apostrophe year (group 4)
        \b(First|Second|Third|Fourth|Fifth|Sixth|Seventh|Eighth|Ninth|Tenth|
        Eleventh|Twelfth|Thirteenth|Fourteenth|Fifteenth|Sixteenth|Seventeenth|
        Eighteenth|Nineteenth|Twentieth|Twenty[- ]?First|Twenty[- ]?Second|
        Twenty[- ]?Third|Twenty[- ]?Fourth|Twenty[- ]?Fifth|Twenty[- ]?Sixth|
        Twenty[- ]?Seventh|Twenty[- ]?Eighth|Twenty[- ]?Ninth|Thirtieth)\b
        """,
        re.IGNORECASE | re.X
    )

    editions = []
    for match in pattern.finditer(conference_name):
        year_4d = match.group(1)
        numeric_ordinal = match.group(2)
        edition_x = match.group(3)
        apostrophe_year = match.group(4)
        textual_ordinal = match.group(5)

        if year_4d:
            editions.append(('year', int(year_4d)))
        elif numeric_ordinal:
            editions.append(('ordinal', int(numeric_ordinal)))
        elif edition_x:
            editions.append(('edition_x', int(edition_x)))
        elif apostrophe_year:
            year = int(apostrophe_year)
            current_year = datetime.now().year
            full_year = 2000 + year if year < (current_year - 2000 + 1) else 1900 + year
            editions.append(('apostrophe_year', full_year))
        elif textual_ordinal:
            key = textual_ordinal.lower().replace('-', '').replace(' ', '')
            editions.append(('textual_ordinal', ORDINAL_WORDS.get(key, None)))

    # Priority order: edition_x > textual_ordinal > ordinal > apostrophe_year > year
    priority_order = ['edition_x', 'textual_ordinal', 'ordinal', 'apostrophe_year', 'year']
    for category in priority_order:
        for ed_type, value in editions:
            if ed_type == category and value is not None:
                return value
    return None

def clean_conference_name(conference_name):
    # Remove edition-related terms to get the base name
    pattern = re.compile(
        r"""
        \b\d{4}\b|                        # 4-digit year
        \d+(?:st|nd|rd|th)\b|             # Numeric ordinal
        Edition\s+\d+|                    # Explicit "Edition X"
        ['’]\d{2}\b|                      # Apostrophe year
        \b(?:First|Second|Third|Fourth|Fifth|Sixth|Seventh|Eighth|Ninth|Tenth|
        Eleventh|Twelfth|Thirteenth|Fourteenth|Fifteenth|Sixteenth|Seventeenth|
        Eighteenth|Nineteenth|Twentieth|Twenty[- ]?First|Twenty[- ]?Second|
        Twenty[- ]?Third|Twenty[- ]?Fourth|Twenty[- ]?Fifth|Twenty[- ]?Sixth|
        Twenty[- ]?Seventh|Twenty[- ]?Eighth|Twenty[- ]?Ninth|Thirtieth)\b
        """,
        re.IGNORECASE | re.X
    )
    cleaned = pattern.sub('', conference_name)
    # Clean up extra spaces and punctuation
    cleaned = re.sub(r'\s{2,}', ' ', cleaned)  # Replace multiple spaces
    cleaned = re.sub(r'[ ,\-]+$', '', cleaned)  # Remove trailing punctuation
    cleaned = re.sub(r'^[ ,\-]+', '', cleaned)  # Remove leading punctuation
    return cleaned.strip()


def assign_proceedings_venues(paper_lst):

    conference_articles = []

    for paper in paper_lst:
        
        if paper['publicationTypes'] != None and  "Conference" in paper['publicationTypes']: 
            conference_articles.append(paper)

    conference_df = pd.DataFrame(conference_articles)
    sub_df = conference_df[['paperId','title','publicationDate','year','venue']]
    sub_df['venue_id'] = conference_df['publicationVenue'].apply(lambda x: x.get('id', None) if x else None)
    sub_df['pages'] = conference_df['journal'].apply(lambda x: x.get('pages', None) if x else None)
    sub_df['proceedings'] = sub_df['venue'].apply(extract_edition)
    sub_df['new_conference_name'] = sub_df['venue'].apply(clean_conference_name)
    df1 = sub_df[~sub_df['proceedings'].isnull()]
    df1 = df1.reset_index(drop=True)
    df2 = sub_df[sub_df['proceedings'].isnull()]
    df2 = df2.reset_index(drop=True)
    grouped = df1.groupby(by=['new_conference_name','proceedings'])

    for g in grouped.groups:
        index = list(grouped.groups[g])
        df1.loc[index,'location'] = fake.city()

    grouped = df2.groupby(by=['year','new_conference_name'])

    conf_count = {}

    for g in grouped.groups:
        index = list(grouped.groups[g])
        
        df2.loc[index,'location'] = fake.city()
        
        if g[1] in conf_count.keys():
            conf_count[g[1]] += 1
            df2.loc[index,'proceedings'] = conf_count[g[1]]
        else:
            conf_count[g[1]] = 3
            df2.loc[index,'proceedings'] = 3

    final_df = pd.concat([df1,df2])
    final_df = final_df.reset_index(drop=True)
    final_df=final_df.drop_duplicates()

    final_df = final_df[['paperId','venue_id','year','proceedings','location', 'pages','new_conference_name']]
    final_df.dropna(subset=['venue_id'], inplace=True)
    final_df=final_df.rename(columns={'proceedings': 'edition'})

    final_df.to_csv('./paper_data/paper_proceedings_location_new.csv',index=False)

    print("Venues and Proceeding created successfully!!!")


def reviewers_metadata():

    with open("./paper_data/paper_reviewers.json",'r') as f:
        paper_reviewers = json.load(f)

    paper_reviewer_metadata = {}

    for paper_id, author_ids in paper_reviewers.items():
        num_reviewers = len(author_ids)
        for i, author_id in enumerate(author_ids, 1):
            if paper_id not in paper_reviewer_metadata:
                paper_reviewer_metadata[paper_id] = {}
            paper_reviewer_metadata[paper_id][f"reviewer_{i}"] = {
                "authorId": author_id,
                "comments": random.choice(peer_reviews),
                "vote": random.choices(review_decisions, weights=[0.8, 0.2])[0],  # 80% accept, 20% reject
            }

    with open("./paper_data/paper_reviewers_metadata.json",'w') as f:
        json.dump(paper_reviewer_metadata,f)

    print("Metadata for Reviews Generated Successfully!!!")


def preprocessing():

    ## Load Papers
    paper_lst = []
    paths = ['paper_data/combined_papers_data.json']
    for path in paths:
        with open(f'./{path}','r') as f:
            data = json.load(f)

        paper_lst.extend(data)
    
    print("Paper Loaded Successfully") 

    assign_reviewer_research_paper(paper_lst)
    assign_keywords_research_paper(paper_lst)
    assign_proceedings_venues(paper_lst)
    reviewers_metadata()


if __name__ == "__main__":

    preprocessing()

    print("Preprocessing Completed Succesfully !!!")









