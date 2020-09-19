import discord
from elasticsearch import Elasticsearch, helpers
import sys
from influxdb import InfluxDBClient
import datetime
import json
import markovify

configFile = open('./config.json', 'r')
config = json.loads(configFile.read())
configFile.close()

influxHost = config['influxHost']
influxPort = config['influxPort']
influxUser = config['influxUser']
influxPassword = config['influxPassword']
elasticHost = config['elasticHost']
prefix = config['prefix']
discordToken = config['discordToken']
databasePrefix = config['databasePrefix']

checkUsers = {}
influx = InfluxDBClient(influxHost, influxPort, influxUser, influxPassword)
elastic =  Elasticsearch([
    {'host':elasticHost}
])
client = discord.Client()

async def handleInfoCommand(args, message, statusMessage):
    # Get our target user, error out if none found
    #
    targetUser=getUserFromInfoArgs(message, args)
    if targetUser is None:
        await statusMessage.edit(content='Couldn\'t find that user. Sorry!')
        return
    targetID = targetUser.id
    # Make multiple InfluxDB queries at one time.
    #
    dbName = '{0}_{1}'.format(databasePrefix, message.guild.id)
    infoQuery = "SELECT COUNT(messageSent)\
     FROM chatMessage\
     WHERE authorID={0}\
     fill(0);\
     \
     SELECT COUNT(messageSent)\
     FROM chatMessage\
     WHERE authorID={0}\
     AND time > now() -7d\
     fill(0);\
     \
     SELECT MEAN(messageLength)\
     FROM chatMessage\
     WHERE authorID={0}\
     AND time > now() - 7d\
     FILL(0);\
     \
     SELECT MOVING_AVERAGE(COUNT(messageSent),9)\
     FROM chatMessage\
     WHERE authorID={0}\
     AND time > now() - 7d\
     GROUP BY time(1d)\
     FILL(0);\
     \
     SELECT COUNT(messageSent)\
     FROM chatMessage\
     WHERE authorID={0}\
     GROUP BY channelName\
     fill(0);\
     \
     SELECT LAST(xp)\
     FROM chatMessage\
     WHERE authorID={0}\
     fill(0)".format(targetID)
    infoResults = influx.query(infoQuery, database=dbName)
    success = True
    resultConstructor = '```'
    # Sort through our results, if nothing was returned, due to the target
    # being a new user, fail gracefully and set it to NULL
    try:
        totalMsgs = list(infoResults[0].get_points())[0]['count'] # Total number of messages
    except Exception as e:
        success = False
        totalMsgs = 'NULL'
        print(f'{e} while grabbing total messages')
    try:
        wkMsgs = list(infoResults[1].get_points())[0]['count'] # Messages this week
    except Exception as e:
        success = False
        wkMsgs = 'NULL'
        print(f'{e} while grabbing this week\'s messages')
    try:
        avgMsgLen = list(infoResults[2].get_points())[0]['mean'] # Average Message length
    except Exception as e:
        success = False
        avgMsgLen = 'NULL'
        print(f'{e} while getting the average message length')
    try:
        avgMsgsWk = list(infoResults[3].get_points())[0]['moving_average'] # Daily average messages
    except Exception as e:
        success = False
        avgMsgsWk = 'NULL'
        print(f'{e} while getting the average daily messages')
    try:
        # We handle parsing this later, so don't bother getting the final number like the rest
        channelBreakdown = infoResults[4] # Breakdown per-channel of messages sent
    except Exception as e:
        success = False
        channelBreakdown = None
        print(f'{e} while getting the channel breakdown')

    try:
        handleXP(message)
        xp = list(infoResults[5].get_points())[0]['last'] # The user's XP. Calculated per 5-minute block of active time
    except Exception as e:
        success = False
        xp = 'NULL'
        print(f'{e} while getting user\'s XP')
    try:
        # Get a markov-chain-generated sentence based on user's activity
        #
        model = buildMarkovModel(targetUser, message)
        sentence = model.make_sentence()
        x = 0
        # Sometimes the model returns nothing
        # Try again until we have something, or we've tried ten times. Whichever comes first
        #
        while sentence is None and x <= 10:
            sentence = model.make_sentence()
            x = x + 1
    except Exception as e:
        success = False
        sentence = 'NULL'
        print(f'{e} while generating markov sentence')

    if totalMsgs is not 'NULL':
        result = ''
        excluded = 0
        breakdown = ''
        # Parse out the message counts and channel names for all channels returned
        #
        for channel in channelBreakdown.items():
            channelName = channel[0][1]['channelName']
            count = int(list(channel[1])[0]['count'])
            ratio = count / totalMsgs
            percent = round(ratio * 100)

            if ratio < 0.05:
                # Group low-activity channels into their own "other" category
                excluded += ratio
            else:
                breakdown += f"#{channelName} ({percent}%)\n"
        excluded_pct = round(excluded * 100)
        breakdown += f'Other(<5%) ({excluded_pct}%)'
    if totalMsgs is not 'NULL' and xp is not 'NULL':
        # Calculate the number of messages sent per XP
        # Roughly the average number of messages per 5 minutes
        #
        print('calculating XP')
        xpPerMsg = round(totalMsgs / xp, 2)
    else:
        xpPerMsg = 'NULL'
    # Build a discord embed with all our collected and processed data
    #
    statEmbed = discord.Embed(title='**Server Activity**')
    statEmbed.set_author(name=targetUser.display_name,icon_url=targetUser.avatar_url)
    statEmbed.add_field(name="**Messages**", value=totalMsgs, inline=True)
    statEmbed.add_field(name="**XP**", value=xp, inline=True)
    statEmbed.add_field(name="**Msgs/XP**", value=xpPerMsg, inline=True)
    statEmbed.add_field(name='**7 Days**', value=wkMsgs,inline=True)
    statEmbed.add_field(name='**Avg. Msg. -7d**', value=round(avgMsgsWk),inline=True)
    statEmbed.add_field(name='**Avg. Msg. Length**', value=round(avgMsgLen), inline=True)
    statEmbed.add_field(name="**Channel Usage**", value=breakdown,inline=False)
    statEmbed.add_field(name="**Randomly Generated Sentence**",value=sentence,inline=True)
    statEmbed.set_footer(text='One XP awarded per 5 minutes of server activity, regardless of message count.\nNULL values are probably because you haven\'t posted enough to provide meaningful data')
    await statusMessage.edit(content='',embed=statEmbed)

def buildMarkovModel(member, message):
    # This queries InfluxDB for a random sample of 1000 messages from the last year from a user
    # then generates a markov chain model for it, and returns the model
    corpus = ''
    query = influx.query(f'SELECT SAMPLE(messageText,100000)\
            FROM chatMessage\
            WHERE serverID={message.guild.id}\
            AND authorID={member.id}\
            AND TIME >= now() - 156w',database=f'{databasePrefix}_{message.guild.id}')
    for item in (list(query.items()[0][1])):
        corpus += f"{item['sample']}\n"
    markovModel = markovify.Text(corpus, state_size=3)
    return markovModel

def getUserFromInfoArgs(message, arg):
    # Takes a message sent, and some preprocessed arguments
    # and pulls out a user object
    #
    if not message.mentions:
        if len(arg) != 0:
            # Either from an ID
            #
            targetMember = message.guild.get_member(int(arg[0]))
        else:
            # Or the author if no argument is supplied
            #
            targetMember = message.author
        if targetMember is not None:
            return targetMember
        else:
            return None
    else:
        # Or a mentioned user
        #
        return message.mentions[0]

def handleXP(message):
    # This calculates and then logs a user's XP
    # XP is awarded for each block of 5 minutes where the user has a message.
    # Rewards sustained participation over bulk messages, doesn't give
    # people who like
    # to split their messages
    # into multiple
    # short messages
    # an advantage
    queryResults = influx.query('SELECT cumulative_sum(max(\"messageSent\"))\
                                      FROM \"chatMessage\"\
                                      WHERE (authorID = {0})\
                                      AND time >= 0ms\
                                      GROUP BY time(5m)\
                                      fill(null)'.format(message.author.id),database='{0}_{1}'.format(databasePrefix, message.guild.id))
    resultItems = list(queryResults.get_points())
    # Declare some variables before writing it to Influx
    #
    measurement = 'chatMessage'
    authorID = message.author.id
    authorName = message.author.name
    serverID = message.guild.id
    serverName = message.guild.name
    dbName = '{0}_{1}'.format(databasePrefix,message.guild.id)
    resultPoint = [{
        "measurement": measurement,
        "tags": {
            "type":"default",
            "authorID":authorID,
            "authorName":authorName,
            "serverID":serverID,
            "serverName":serverName
        },
        "fields": {
            "authorID":authorID,
            "authorName":authorName,
            "serverID":serverID,
            "serverName":serverName,
            "xp":len(resultItems)
        }
    }]
    # Write the point
    #
    try:
        influx.write_points(resultPoint, database=dbName,protocol=u'json')
    except Exception as e:
        print('ERROR: Couldnt log user\'s XP to influxdb')
        print(e)


def buildElasticDocFromMessage(message):
    # This takes a message object and returns some data pre-formatted for ElasticSearch
    #
    indexName = '{0}_{1}'.format(databasePrefix, message.guild.id)
    attachments = []
    for attachment in message.attachments:
        attachments.append(attachment.url)
    measurement = 'chatMessage'
    authorID = message.author.id
    authorName = message.author.name
    serverID = message.guild.id
    serverName = message.guild.name
    channelID = message.channel.id
    channelName = message.channel.name
    messageID = message.id
    messageTextRaw = message.content
    messageText = message.clean_content
    messageLength = len(message.clean_content)
    timestamp = int((message.created_at - datetime.datetime(1970, 1, 1)).total_seconds()) # ES's default time precision can be seconds or millisenconds. We pick seconds
    createObj  = {
        "create": {
            "_index":indexName,
            "_id":message.id
        }
    }
    docObject = {
        "type":'default',
        "timestamp":timestamp,
        "messageSent":1,
        "authorID":authorID,
        "authorName":authorName,
        "messageID":messageID,
        "messageLength":messageLength,
        "messageTextRaw":messageTextRaw,
        "messageText":messageText,
        "channelID":channelID,
        "channelName":channelName,
        "serverID":serverID,
        "serverName":serverName,
        "attachments":', '.join(attachments)
    }
    final = '{0}\n{1}'.format(json.dumps(createObj), json.dumps(docObject))
    return final


def buildInfluxPointFromMessage(message):
    # Grab a bunch of variables to assemble influx-compatible data point, formatted as JSON
    #
    attachments = []
    for attachment in message.attachments:
        attachments.append(attachment.url)
    measurement = 'chatMessage'
    authorID = message.author.id
    authorName = message.author.name
    serverID = message.guild.id
    serverName = message.guild.name
    channelID = message.channel.id
    channelName = message.channel.name
    messageID = message.id
    messageTextRaw = message.content
    messageText = message.clean_content
    messageLength = len(message.clean_content)
    timestamp = int((message.created_at - datetime.datetime(1970, 1, 1)).total_seconds() * 1000000000) # Influx's default time precision is in freaking nanoseconds
    object = {
        "measurement": measurement,
        "tags": {
            "type":"default",
            "authorID":authorID,
            "authorName":authorName,
            "serverID":serverID,
            "serverName":serverName,
            "channelID":channelID,
            "channelName":channelName,
        },
        "time":timestamp,
        "fields": {
            "messageSent":1,
            "authorID":authorID,
            "authorName":authorName,
            "messageID":messageID,
            "messageLength":messageLength,
            "messageTextRaw":messageTextRaw,
            "messageText":messageText,
            "channelID":channelID,
            "channelName":channelName,
            "serverID":serverID,
            "serverName":serverName,
            "attachments":', '.join(attachments)
        }
    }
    return object

def checkListOfDict(list, key, value):
    # Helper function for parsing lists of dictionaries
    #
    for dict in list:
        if dict[key] == value:
            return True
    return False

async def influxDBInit(guildID):
    # Initialize an influxDB database for every guild we're in
    #
    dbName = '{0}_{1}'.format(databasePrefix,guildID)
    if not checkListOfDict(influx.get_list_database(), 'name', dbName):
        influx.create_database(dbName)
        influx.switch_database(dbName)
        print('Initialized influxDB database {0}'.format(dbName))
    else:
        influx.switch_database(dbName)

async def elasticInit(guildID):
    # Initialize an elasticDB index for every guild we're in
    #
    indexName = '{0}_{1}'.format(databasePrefix, guildID)
    elastic.indices.create(index=indexName, ignore=400)
    print('Initialized ElasticSearch index {0}'.format(indexName))

@client.event
    # Initialize new databases and indices for new guilds we join
    #
async def on_guild_join(guild):
    print('I joined a new guild!')
    print('Name:{0}, ID:{1}'.format(guild.name,guild.id))
    influxDBinit(guild.id)
    elasticInit(guild.id)

@client.event
async def on_ready():
    # Initialize databases and indices on startup
    #
    print('Connected as {0.user}'.format(client))
    for guild in client.guilds:
        await influxDBInit(guild.id)
        await elasticInit(guild.id)
    print('Finished initializing databases.')

@client.event
async def on_message(message):
    # Start by building Influx and Elastic-compatible data points out of the message
    # Then store them
    #
    influxBatchBuffer = []
    influxBatchBuffer.append(buildInfluxPointFromMessage(message))
    dbName =  '{0}_{1}'.format(databasePrefix,message.guild.id)
    if message.author.bot:
        return
    try:
        influx.write_points(influxBatchBuffer,database=dbName,protocol=u'json')
    except:
        print('Couldn\'t write to InfluxDB')
    try:
        elastic.bulk(buildElasticDocFromMessage(message))
    except:
        print('Couldn\'t write to ElasticSearch')
    # Run handleXP every ten messages for every user
    #
    if message.guild.id not in checkUsers:
        checkUsers.update({message.guild.id: {}})
    if message.author.id not in checkUsers[message.guild.id]:
        checkUsers[message.guild.id].update({message.author.id: '1'})
    else:
        lastCheck = int(checkUsers[message.guild.id][message.author.id])
        checkUsers[message.guild.id][message.author.id] = lastCheck + 1
    if checkUsers[message.guild.id][message.author.id] == 10:
        checkUsers[message.guild.id][message.author.id] = 0
        handleXP(message)

    if message.clean_content.find(prefix) == 0 :
        # Handle the message if it's a command. Split it into an array and
        # treat every word like an argument
        # If it starts with a known command, act on it
        #
        args = message.clean_content.split(' ')
        if args[0] == f'{prefix}markov':
            args.pop(0)
            target = getUserFromInfoArgs(message, args)
            if target is not None:
                model = buildMarkovModel(target, message)
                sentence = model.make_sentence()
                x = 0
                while sentence is None and x <= 10:
                    # Same deal as before. Try to get a sentence from our markov model
                    # give up after ten tries
                    #
                    sentence = model.make_sentence()
                    x = x + 1
                # Generate an embed for the markov sentence
                embed = discord.Embed(title='Markov')
                embed.add_field(name=f'{target.display_name}\'s random sentence:',value=sentence)
                await message.channel.send(embed=embed)
            return
        if args[0] == '{0}info'.format(prefix):
            args.pop(0)
            statusMessage = await message.channel.send("Hold tight, fetching some data...")
            try:
                await handleInfoCommand(args, message, statusMessage)
            except:
                await statusMessage.edit(content='Something went wrong. Try again later or ping Zack.')
client.run(discordToken)
