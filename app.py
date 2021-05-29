import itertools
import boto3
from boto3.dynamodb.conditions import Key, Attr
import sys

dynamodb = boto3.resource('dynamodb')
TABLE = "Courses"

def get_description(subject: str, level: int) -> str:
    """Helper method to create dummy descriptions"""
    prefix = "Introduction to "
    if level > 200:
        prefix = "Intermediate "
    if level > 400:
        prefix = "Advanced "
    
    title = prefix + subject + " at UMGC"
    return title
    
def get_credits(level: int):
    if level > 300:
        return 4
    else:
        return 3
    
def create_data() -> None:
    table = dynamodb.Table(TABLE)
    names = ["ENG", "SDEV"]
    numbers = list(range(100, 600, 100))
    for course_id, (subject, num) in enumerate(list(itertools.product(names, numbers))):
        table.put_item(Item={
               'CourseID': course_id,
               'Subject': subject,
               'CatalogNbr': str(num),
               'Title': get_description(subject, num),
               'NumCredits': get_credits(num)
            })

def create_table() -> dict:
    """Create the classes table and return the response dictionary"""
    table = dynamodb.create_table(
        TableName=TABLE,
        KeySchema=[
            {
                'AttributeName': 'CourseID',
                'KeyType': 'HASH'  #Partition key
            },
            {
                'AttributeName': 'Subject',
                'KeyType': 'RANGE'  #Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'CourseID',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'Subject',
                'AttributeType': 'S'
            },
    
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

def yes_no_menu(message: str) -> bool:
    """Generic Y/N menu which uses a custom prompt message"""
    valid_choice = False
    while not valid_choice:
        choice = input(f"{message} (Y/N)")
        if choice is None or choice.strip() == "":
            print("Invalid choice, try again")
        elif choice.lower() == "y":
            return True
        elif choice.lower() == "n":
            return False
        else:
            print("Invalid choice, try again")
            

def run_search(subject: str, catalog: str) -> bool:
    """Search the database for course and print the result"""
    try:
        table = dynamodb.Table(TABLE)
        query = Key('CourseID').between(0, 10) &  Attr('Subject').eq(subject) & Attr('CatalogNbr').eq(catalog)
        select = "Subject, CatologNo, Title"
    
        response = table.scan(FilterExpression=query)
        
        res = response['Items']
        items = res[0]
    
        print(f'The title of {items["Subject"]} {items["CatalogNbr"]} is {items["Title"]}.')
        return True
    except Exception as ex:
        print("No results found")
        return False

def search() -> bool:
    subject = input("Enter a subject: ")
    if subject is None or subject.strip() == "":
        print("Invalid input, running search again")
        return False
    
    catalog = input("Enter a CatalogNbr: ")
    if catalog is None or catalog.strip() == "":
        print("Invalid input, running search again")
        return False
        
    run_search(subject, catalog)
    
    run_again = yes_no_menu("Run another search?")
    return not run_again
    

def main():
    if "--create-tables" in sys.argv:
        create_table()
    elif "--insert-data" in sys.argv:
        create_data()
    else:
        print("--No command line option chosen, search function automatically selected--")    
        done_search = False
        while not done_search:
            done_search = search()
    sys.exit(0)

if __name__ == "__main__":
    main()