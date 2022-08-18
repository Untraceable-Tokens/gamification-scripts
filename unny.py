import csv
from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F


spark = (
    SparkSession.builder 
    .appName("unny")
    .getOrCreate()
)

df_people = spark.read.csv("unny_people.csv", header=True)
df_people = df_people.filter("groups = 'Attendees, Speakers'").select("id", "first_name", "last_name", "company", "title")
people = df_people.collect()
# spark.sparkContext.parallelize(people).foreach(lambda row: print(get_session_description(row.title, row.description, row.speaker_ids, row.moderator_ids)))
spark.sparkContext.broadcast(people)

df_sessions = spark.read.csv("unny_sessions.csv", header=True)
df_sessions = df_sessions.filter(F.col("speaker_ids").isNotNull()).filter("date = '2022-08-09' OR date = '2022-08-10'")

print(people)
speaker_row = [p for p in people][0]
print(speaker_row)
print(speaker_row.first_name)

def get_session_speakers(speaker_ids) -> str:
    """
    Returns a string of the session speakers.
    """
    if not speaker_ids or speaker_ids == "":
        return None

    session_speakers = ""
    for speaker_id in speaker_ids.split(","):
        speaker_id = speaker_id.strip()
        speaker_row = [p for p in people if p.id == speaker_id]
        if len(speaker_row) > 0:
            speaker_row = speaker_row[0]
            first_name = speaker_row.first_name
            last_name = speaker_row.last_name
            company = speaker_row.company
            title = speaker_row.title
            if last_name and last_name != "":
                session_speakers += f"{first_name} {last_name}"
            else:
                session_speakers += f"{first_name}"

            if company and company != "":
                if title and title != "":
                    session_speakers += f" ({company} - {title}), "
                else:
                    session_speakers += f" ({company}), "
            else:
                session_speakers += f", "
    if session_speakers != "":
        session_speakers = session_speakers[:-2]
    return session_speakers

def get_session_moderators(moderator_ids) -> str:
    """
    Returns a string of the session moderators.
    """

    if not moderator_ids or moderator_ids == "":
        return None

    session_moderators = ""
    for moderator_id in moderator_ids.split(","):
        moderator_id = moderator_id.strip()
        moderator_row = [p for p in people if p.id == moderator_id]
        if len(moderator_row) > 0:
            moderator_row = moderator_row[0]
            first_name = moderator_row.first_name
            last_name = moderator_row.last_name
            company = moderator_row.company
            title = moderator_row.title
            if last_name and last_name != "":
                session_moderators += f"{first_name} {last_name}"
            else:
                session_moderators += f"{first_name}"

            if company and company != "":
                if title and title != "":
                    session_moderators += f" ({company} - {title}), "
                else:
                    session_moderators += f" ({company}), "
            else:
                session_moderators += f", "
    if session_moderators != "":
        session_moderators = session_moderators[:-2]
    return session_moderators

def get_session_description(title, description, speaker_ids, moderator_ids) -> str:
    """
    Returns a string of the session description.
    """

    print(f"title: {title}")
    print(f"description: {description}")
    print(f"speaker_ids: {speaker_ids}")
    print(f"moderator_ids: {moderator_ids}")

    print("getting speakers")
    speakers = get_session_speakers(speaker_ids)
    print("getting moderators")
    moderators = get_session_moderators(moderator_ids)

    ret = str(title)
    if description and description != "":
        ret += f" - {description}"
    if speakers and speakers != "":
        ret += f"\n\nSpeaker(s): {speakers}"
    if moderators and moderators != "":
        ret += f"\n\nModerator(s): {moderators}"
    return ret
        


def get_session_title(title) -> str:
    """
    Returns a string of the session title.
    """
    if len(title) > 128:
        title = title[:125]
        last_space = title.rfind(" ")
        if last_space > 0:
            title = title[:last_space]
        title += "..."
    return title


get_title_udf = F.udf(get_session_title, F.StringType())
get_description_udf = F.udf(get_session_description, F.StringType())


df_sessions = df_sessions.withColumn("title", get_title_udf(F.col("session_name")))
df_sessions = df_sessions.withColumn("new_description", get_description_udf(F.col("session_name"), F.col("description"), F.col("speaker_ids"), F.col("moderator_ids")))


df_sessions.write.csv("unny_sessions_with_description.csv", header=True, mode="overwrite")
# df_sessions.toPandas().to_csv("./unny_sessions_with_description.csv", index=False, header=True)

spark.stop()


