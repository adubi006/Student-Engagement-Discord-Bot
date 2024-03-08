import os
import pytz
import random
import discord
import gspread
import asyncio
import pandas as pd
from datetime import datetime
from discord.ext import tasks
from dotenv import load_dotenv
from discord.ext import commands
from collections import defaultdict
from oauth2client.service_account import ServiceAccountCredentials

# Load environment variables from .env file
load_dotenv("mavi.env")
ds_token = os.getenv("BOT_TOKEN")
guild_id = int(os.getenv("GUILD_ID"))
channel_id = int(os.getenv("CHANNEL_ID"))

# Define Mavi's intents
intents = discord.Intents.default()
intents.typing = False
intents.presences = False
intents.messages = True
intents.members = True

# Initialize Mavi with intents and specify the command prefix
Mavi = commands.Bot(command_prefix='!', intents=intents)

# Define and include Google Sheets scope and credentials
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(r'C:\Users\Hp\Downloads\explore-sed-e91f95f5477f.json', scope)
gc = gspread.authorize(creds)

Mavis_Records = "exploreSED"
QTN = "QTN"
ANS = "ANS"
Student_details = "Student_details"
Intro_Mavi = "Intro_Mavi"
Sent_Questions = "Sent_Questions"


# Get the questions ('QTN') worksheet
qtn_worksheet = gc.open(Mavis_Records).worksheet(QTN)
ans_worksheet = gc.open(Mavis_Records).worksheet(ANS)

# Get a list of questions and their corresponding IDs from the 'QTN' sheet
questions_data = qtn_worksheet.get_all_records()
ans_data = ans_worksheet.get_all_records()

@Mavi.event
async def on_ready():
    print(f'Logged in as {Mavi.user.name}')
    
    guild = discord.utils.get(Mavi.guilds, id=guild_id)
    channel = Mavi.get_channel(channel_id)

    # MAVI INTRODUCES HERSELF TO THE MEMBERS
    intro = gc.open(Mavis_Records).worksheet(Intro_Mavi).cell(1, 1).value
    await channel.send(intro)
    

    
    # Return the date and time when a member joined the channel
    join_time = None
    for member in [member for member in channel.members if not member.bot]:
        join = member.joined_at

        if join:
            join_time = join.strftime('%Y-%m-%d %H:%M:%S')

        await member.send(intro)
        student_details = gc.open(Mavis_Records).worksheet("Student_details")
        student_details.append_row([member.name, str(member.id), join_time, ""])

    send_questions.start()


@Mavi.event
async def on_member_join(member):

    intro = gc.open(Mavis_Records).worksheet(Intro_Mavi).cell(1, 1).value
    await member.create_dm()
    await member.dm_channel.send(f'Hi {member.name}, {intro}')


# Task that sends questions to students every Monday, Wednesday, and Friday
@tasks.loop(minutes=1)
async def send_questions():
    
    guild = discord.utils.get(Mavi.guilds, id=guild_id)
    channel = Mavi.get_channel(channel_id)

    ans_wrksheet = gc.open(Mavis_Records).worksheet(ANS)
    
    student_ids = ans_worksheet.col_values(1)  
    question_ids = ans_worksheet.col_values(2)  

    ans_dat = list(zip(student_ids, question_ids))
        
    # Create a list of members to send questions to
 #   members_send = [member for member in guild.members if not member.bot and not member.guild_permissions.administrator and "mentor" not in [role.name.lower() for role in member.roles]]
  
    members_send = [member for member in guild.members if not member.bot]

    ## Iterate through members and send questions concurrently
    tasks = []
    for member in members_send:
        student_id = str(member.id)

        sent_question_ids = set(q_id for s_id, q_id in ans_dat[1:] if s_id == student_id)
        available_qtn_ids = set(q_data['Question_ID'] for q_data in questions_data) - set(sent_question_ids)

        if available_qtn_ids:
            question_id = random.choice(list(available_qtn_ids))
            question_text = next(q_data['Question'] for q_data in questions_data if q_data['Question_ID'] == question_id)
            task = get_response(member, question_text, question_id)
            tasks.append(task)
            
        else:
            # Handle the case when no available questions are left
            print(f'No more questions available for {member.name}')
            
            
    # Wait for all questions to be sent
    await asyncio.gather(*tasks)



# Function to calculate the awarded points based on response time
def calculate_awarded_points(question_sent_time, response_received_time):
    time_difference = response_received_time - question_sent_time
    hours_difference = time_difference.total_seconds() / 3600

    if 0 <= hours_difference < 3:
        return 5
    elif 3 <= hours_difference < 6:
        return 3
    elif 6 <= hours_difference < 9:
        return 2
    elif 9 <= hours_difference < 24:
        return 1
    else:
        return 0



async def get_response(member, question, question_id):

    guild = discord.utils.get(Mavi.guilds, id=guild_id)
    channel = Mavi.get_channel(channel_id)
    
    # Get the date and time (WAT) the member sent a response
    wat_time = pytz.timezone('Africa/Lagos')
    timestamp_wat = datetime.now(wat_time)
    timestamp = timestamp_wat.strftime('%Y-%m-%d %H:%M:%S')
    current_date = timestamp_wat.strftime('%Y-%m-%d')

    try:
        await member.send(question)
        print(f'Sent a question to {member.name}')

        # Record the time the question was sent
        question_sent_time = datetime.now(wat_time)

        # Wait for a response from the member
        def check_response(msg):
            return msg.author == member and msg.content

        response = await Mavi.wait_for('message', check=check_response, timeout=60)

        # Calculate the time difference
        response_received_time = datetime.now(wat_time)
        time_difference = response_received_time - question_sent_time
        hours, remainder = divmod(time_difference.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)

        formatted_time_difference = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

        # Calculate the awarded points
        awarded_points = calculate_awarded_points(question_sent_time, response_received_time)

        # Format the response message
        response_message = f"Thank you for your answer.\nYour answer came in {formatted_time_difference} after the question was sent."
        response_message += f"\nYou have been awarded {awarded_points} points for your timely response."

        # Send the response message
        await member.send(response_message)

        # Store the awarded points in the 'Points' Google Sheets worksheet
        points_worksheet = gc.open(Mavis_Records).worksheet("Points")
        points_worksheet.append_row([str(member.id), question_id, int(awarded_points)])

        # Record the response in the ANS sheet
        answers_worksheet = gc.open(Mavis_Records).worksheet("ANS")
        answers_worksheet.append_row([str(member.id), question_id, response.content, timestamp])

        # Calculate aggregated points for Leaderboard
        # points_worksheet = gc.open(Mavis_Records).worksheet("Points")
        # agg_worksheet = gc.open(Mavis_Records).worksheet("Aggregate Points")
        # points(points_worksheet, agg_worksheet)
        
        # leaders = await leaderboard(agg_worksheet, guild, channel)
        # for leader in leaders:
        #    await channel.send(leader)

        print(f'Received response from {member.name}: {response.content}')
    except discord.Forbidden:
        print(f'Could not send a question to {member.name} (DMs disabled)')
    except discord.HTTPException as e:
        print(f"Failed to send a message to {member.name}: {e}")
    except asyncio.TimeoutError:
        answers_worksheet = gc.open(Mavis_Records).worksheet("ANS")
        answers_worksheet.append_row([str(member.id), question_id, "No Response", timestamp])
        print(f'No response received from {member.name} within the timeout.')
        # Update'Points' worksheet for members who did not respond
        points_worksheet = gc.open(Mavis_Records).worksheet("Points")
        points_worksheet.append_row([str(member.id), question_id, 0])


def points(get_points_sheet, aggregate_worksheet):
    
    # Read data from the 'Points' sheet
    points_data = get_points_sheet.get_all_records()
    
    # Create a dictionary to aggregate scores by student_ID
    score_aggregates = defaultdict(int)

    # Iterate through the data and aggregate scores
    for record in points_data:
        student_id = record["STUDENT_ID"]
        score = record["AWARDED_POINTS"]
        
        if score:
            score_aggregates[student_id] += int(score)

    # Read existing data from the 'Aggregate Points' sheet
    existing_data = aggregate_worksheet.get_all_records()

    # Create a set to keep track of unique student IDs
    unique_student_ids = set()

    # Update the unique student IDs and total points in the 'Aggregate Points' sheet
    for record in existing_data:
        student_id = record.get("STUDENT_ID")
        if student_id:
            unique_student_ids.add(str(student_id))

    # Add any new unique student IDs to the 'Aggregate Points' sheet and update their total points
    for student_id, total_score in score_aggregates.items():
        if student_id not in unique_student_ids:
            # Add a new record for a unique student ID
            aggregate_worksheet.append_row([str(student_id), total_score])
        else:
            # Update the total points for an existing student ID
            for record in existing_data:
                if record.get("STUDENT_ID") == str(student_id):
                    record["total_scores"] = total_score
                    aggregate_worksheet.update(f'A{existing_data.index(record)}', [[str(student_id), total_score]])

    print("Aggregated data has been updated in the 'Aggregate Points' worksheet.")

async def leaderboard(aggregate_worksheet, guild, chan):
   
    agg_points_data = aggregate_worksheet.get_all_records()

    sorted_data = sorted(agg_points_data, key=lambda x: x['TOTAL_POINT'], reverse=True)
    
    unique_student_ids = set()
    top_records = []

    for record in sorted_data:
        student_id = record['STUDENT_ID']
        
        # Check if the student ID is not in the set of unique student IDs
        if student_id not in unique_student_ids:
            unique_student_ids.add(student_id)
            top_records.append(record)
        
        # If we have collected the top 10 unique student records, break the loop
        if len(top_records) == 10:
            break

    messages = []
    
    # Fetch Discord members
    members = chan.members

    # Send messages for each top record
    for record in top_records:
        student_id = record['STUDENT_ID']
        total_points = record['TOTAL_POINT']

        # Find the Discord member with a matching ID
    member = discord.utils.get(members, id=student_id)

    if member:
        member_name = member.display_name
        message = f"Yay! Let's give it up for these fast fingers:\n"
        message += f"\nName: {member_name}, \nTotal Points: {total_points}"
        messages.append(message)

    return messages

    
    
@Mavi.event
async def on_message(message):
    channel = Mavi.get_channel(channel_id)
    
    #courtesy = ['You are welcome!', 'You are most welcome!', 'The pleasure is all mine', 'Any time', 'For shooo']
   # courtesy_send = random.choice(courtesy)
    
    if message.author == Mavi.user:
        return

    if message.content.startswith('Hello'):
        await message.channel.send('Hello!')

    #if message.content.startswith('Thank'):
    #    await channel.send(courtesy_send)

# Run the bot
Mavi.run(ds_token)
