import pymysql
import pandas as pd
import requests
from tqdm import tqdm
tqdm.pandas()
import pandas as pd
import random
import time
api_key='RGAPI-2ada0acf-3e59-436b-81a4-9b9c18582719'
champurl='https://ddragon.leagueoflegends.com/cdn/13.24.1/data/ko_KR/champion.json'
champlist=requests.get(champurl).json()
champNameK=[]
champNameE=list(champlist['data'])
for i in range(len(champNameE)):
    champNameK.append(champlist['data'][champNameE[i]]['name'])
api_key='RGAPI-2ada0acf-3e59-436b-81a4-9b9c18582719'
spellurl='https://ddragon.leagueoflegends.com/cdn/13.24.1/data/ko_KR/summoner.json'
spell=requests.get(spellurl).json()
namelist=list(spell['data'].keys())
spellList=[]
itemurl='https://ddragon.leagueoflegends.com/cdn/13.24.1/data/ko_KR/item.json'
items=requests.get(itemurl).json()
itemkey=list(items['data'])
itemlist=[]
for i in range(len(namelist)):
    spellList.append([spell['data'][namelist[i]]['name'],spell['data'][namelist[i]]['key']])
for i in range(len(itemkey)):
    itemlist.append([items['data'][itemkey[i]]['name'],itemkey[i]])
print('resource complet')
def connect_mysql(db='lol_data'):
    conn = pymysql.connect(host='svc.sel4.cloudtype.app', port=32233, user='takealook', password='tmddk0908', db=db)
    return conn
def sql_execute(conn,query):
    cursor = conn.cursor()
    cursor.execute(query)
    result=cursor.fetchall()
    return result
def sql_execute_dict(conn,query):
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(query)
    result=cursor.fetchall()
    return result
def get_rawdata(tier):
    division_list = ['I', 'II', 'III', 'IV']
    lst = []
    IdLst = []
    sid = []
    pid = []
    mid = []
    dl = []
    i = 0
    page = random.randrange(1, 20)
    print('get summonerId...')
    for division in tqdm(division_list):
        url1 = f'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page}&api_key={api_key}'
        res = requests.get(url1).json()
        #   if isinstance(res, list):
        lst += random.sample(res, 2)
    for i in range(len(lst)):
        sid.append(lst[i]['summonerId'])
    print('get puuid')
    for i in tqdm(range(len(lst))):
        url2 = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/{sid[i]}?api_key={api_key}'
        pid.append(requests.get(url2).json()['puuid'])
    print('get matchid')
    for i in tqdm(range(len(pid))):
        url3 = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{pid[i]}/ids?queue=420&start=0&count=20&api_key={api_key}'
        res3 = requests.get(url3).json()
        # if isinstance(res3, list):
        mid.append(random.sample(res3, 4))
    print(mid)
    print('get data')
    for i in tqdm(range(len(mid))):
        for j in range(len(mid[i])):
            url4 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{mid[i][j]}?api_key={api_key}'
            url5 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{mid[i][j]}/timeline?api_key={api_key}'
            matches = requests.get(url4).json()
            timelines = requests.get(url5).json()
            tier_list = []
            tdata = matches['info']['participants']
            summonerId = 'summonerId'
            for k in range(10):
                tierurl = f'https://kr.api.riotgames.com/lol/league/v4/entries/by-summoner/{tdata[k][summonerId]}?api_key={api_key}'
                tierdata=requests.get(tierurl).json()
                print(tierdata)
                for l in range(len(tierdata)):
                        if(tierdata[l]['queueType']=='RANKED_SOLO_5x5'):
                            tier_list.append(tierdata[l])
                print("========================")
                time.sleep(1)
            dl.append([mid[i][j],tier_list, matches, timelines])
    df = pd.DataFrame(dl, columns=['match_id','tier_list', 'matches', 'timelines'])
    print('작업완료')
    print(df)
    return df
def get_match_timeline_df(df):
    # df를 한개로 만들기
    df_creater = []
    print('소환사 스텟 생성중.....')
    for i in tqdm(range(len(df))):
        # matches 관련된 데이터
#         try:
            if df.iloc[i].matches['info']['gameDuration'] > 900:   # 게임시간이 15분이 안넘을경우에는 패스하기
                for j in range(len(df.iloc[i].matches['info']['participants'])):
                    # print(j)
                    tmp = []
                    tmp.append(df.iloc[i].match_id)
                    tmp.append(df.iloc[i].matches['info']['gameDuration'])
                    tmp.append(df.iloc[i].matches['info']['gameVersion'])
                    try:
                        tmp.append(df['tier_list'][i][j]['tier'])
                    except:
                        tmp.append('unRank')
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summonerName'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summonerLevel'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['participantId'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['championName'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['champExperience'])
                    for k in range(len(spellList)):
                        if(int(spellList[k][1])==df.iloc[i].matches['info']['participants'][j]['summoner1Id']):
                            tmp.append(spellList[k][0])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summoner1Casts'])
                    for k in range(len(spellList)):
                        if (int(spellList[k][1]) == df.iloc[i].matches['info']['participants'][j]['summoner2Id']):
                            tmp.append(spellList[k][0])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summoner2Casts'])
                    for s in range(len(itemkey)):
                        if(int(itemkey[s])==df.iloc[i].matches['info']['participants'][j][f'item0']):
                            tmp.append(items['data'][itemkey[i]]['name'])
                            break;
                    if(int(itemkey[s])!=df.iloc[i].matches['info']['participants'][j]['item0']):
                        tmp.append('아이템없음')
                    for s in range(len(itemkey)):
                        if(int(itemkey[s])==df.iloc[i].matches['info']['participants'][j][f'item1']):
                            tmp.append(items['data'][itemkey[s]]['name'])
                            break;
                    if (int(itemkey[s]) != df.iloc[i].matches['info']['participants'][j]['item1']):
                        tmp.append('아이템없음')
                    for s in range(len(itemkey)):
                        if(int(itemkey[s])==df.iloc[i].matches['info']['participants'][j][f'item2']):
                            tmp.append(items['data'][itemkey[s]]['name'])
                            break;
                    if (int(itemkey[s]) != df.iloc[i].matches['info']['participants'][j]['item2']):
                        tmp.append('아이템없음')
                    for s in range(len(itemkey)):
                        if(int(itemkey[s])==df.iloc[i].matches['info']['participants'][j][f'item3']):
                            tmp.append(items['data'][itemkey[s]]['name'])
                            break;
                    if (int(itemkey[s]) != df.iloc[i].matches['info']['participants'][j]['item3']):
                        tmp.append('아이템없음')
                    for s in range(len(itemkey)):
                        if(int(itemkey[s])==df.iloc[i].matches['info']['participants'][j][f'item4']):
                            tmp.append(items['data'][itemkey[s]]['name'])
                            break;
                    if (int(itemkey[s]) != df.iloc[i].matches['info']['participants'][j]['item4']):
                        tmp.append('아이템없음')
                    for s in range(len(itemkey)):
                        if(int(itemkey[s])==df.iloc[i].matches['info']['participants'][j][f'item5']):
                            tmp.append(items['data'][itemkey[s]]['name'])
                            break;
                    if (int(itemkey[s]) != df.iloc[i].matches['info']['participants'][j]['item5']):
                        tmp.append('아이템없음')
                    for s in range(len(itemkey)):
                        if(int(itemkey[s])==df.iloc[i].matches['info']['participants'][j][f'item6']):
                            tmp.append(items['data'][itemkey[s]]['name'])
                            break;
                    if (int(itemkey[s]) != df.iloc[i].matches['info']['participants'][j]['item6']):
                        tmp.append('아이템없음')
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamPosition'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamId'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['win'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['kills'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['deaths'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['assists'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageDealtToChampions'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageTaken'])
            #timeline 관련된 데이터
                    for k in range(5,26):
                        try:
                            tmp.append(df.iloc[i].timelines['info']['frames'][k]['participantFrames'][str(j+1)]['totalGold'])
                        except Exception as e:
                            tmp.append(0)
                    df_creater.append(tmp)
                    print(tmp)
#         except Exception as e:
#             print("error",e)
#             continue
    columns = ['gameId', 'gameDuration', 'gameVersion','tier', 'summonerName', 'summonerLevel', 'participantId', 'championName', 'champExperience',
                'spell1Name','spell1Count','spell2Name','spell2Count', 'item0','item1','item2','item3','item4','item5','item6','teamPosition','teamId','win','kills','deaths', 'assists', 'totalDamageDealtToChampions', 'totalDamageTaken',
               'g_5','g_6','g_7','g_8','g_9','g_10','g_11','g_12','g_13','g_14','g_15','g_16','g_17','g_18','g_19', 'g_20', 'g_21','g_22','g_23','g_24','g_25']
    print(len(columns))
    print(len(tmp))
    print(tmp)
    df = pd.DataFrame(df_creater,columns = columns).drop_duplicates()
    print('df 제작이 완료되었습니다. 현재 df의 수는 %d 입니다'%len(df))
    return df
def insert_matches_timelines_mysql(row, conn):

    # lambda를 이용해서 progress_apply를 통해 insert할 구문 만들기
    query = (
        f'insert into semi(gameId, gameDuration, gameVersion,tier, summonerName, summonerLevel, participantId,'
        f'championName, champExperience, spell1Name, spell1Count, spell2Name, spell2Count,'
        f'item0, item1, item2, item3, item4, item5, item6,'
        f'teamPosition, teamId, win, kills, deaths, assists,'
        f'totalDamageDealtToChampions, totalDamageTaken, g_5, g_6, g_7, g_8, g_9, g_10, g_11, g_12 ,g_13,g_14,'
        f'g_15, g_16, g_17, g_18, g_19, g_20, g_21, g_22, g_23, g_24, g_25)'
        f'values(\'{row.gameId}\',{row.gameDuration}, \'{row.gameVersion}\',\'{row.tier}\', \'{row.summonerName}\',{row.summonerLevel},'
        f' \'{row.participantId}\',\'{row.championName}\',{row.champExperience},'
        f'\'{row.spell1Name}\',{row.spell1Count},\'{row.spell2Name}\',{row.spell2Count},'
        f'\'{row.item0}\',\'{row.item1}\',\'{row.item2}\',\'{row.item3}\',\'{row.item4}\',\'{row.item5}\',\'{row.item6}\','
        f'\'{row.teamPosition}\', {row.teamId}, \'{row.win}\', {row.kills}, {row.deaths}, {row.assists},'
        f'{row.totalDamageDealtToChampions},{row.totalDamageTaken},{row.g_5},{row.g_6},{row.g_7},{row.g_8},'
        f'{row.g_9},{row.g_10},{row.g_11},{row.g_12},{row.g_13},{row.g_14},{row.g_15},{row.g_16},{row.g_17},'
        f'{row.g_18},{row.g_19},{row.g_20},{row.g_21},{row.g_22},{row.g_23},{row.g_24},{row.g_25})'
        f'ON DUPLICATE KEY UPDATE '
        f'gameId = \'{row.gameId}\', gameDuration = {row.gameDuration}, gameVersion = \'{row.gameVersion}\', summonerName= \'{row.summonerName}\','
        f'summonerLevel = {row.summonerLevel},participantId = {row.participantId},championName = \'{row.championName}\',champExperience = {row.champExperience},' 
        f'spell1Name=\'{row.spell1Name}\',spell1Count={row.spell1Count},spell2Name=\'{row.spell2Name}\',spell2Count={row.spell2Count},item0=\'{row.item0}\',item1=\'{row.item1}\',item2=\'{row.item2}\',item3=\'{row.item3}\',item4=\'{row.item4}\',item5=\'{row.item5}\',item6=\'{row.item6}\','
        f'teamPosition = \'{row.teamPosition}\', teamId = {row.teamId},win = \'{row.win}\','
        f'kills = {row.kills}, deaths = {row.deaths}, assists = {row.assists}, totalDamageDealtToChampions = {row.totalDamageDealtToChampions},'
        f'totalDamageTaken = {row.totalDamageTaken},g_5 = {row.g_5},g_6 = {row.g_6},g_7 = {row.g_7},g_8 = {row.g_8},g_9 = {row.g_9},'
        f'g_10 = {row.g_10},g_11 = {row.g_11},g_12 = {row.g_12},g_13 = {row.g_13},g_14 = {row.g_14},g_15 = {row.g_15},g_16 = {row.g_16},g_17 = {row.g_17},'
        f'g_18 = {row.g_18},g_19 = {row.g_19},g_20 = {row.g_20},g_21 = {row.g_21},g_22 = {row.g_22},g_23 = {row.g_23},g_24 = {row.g_24},g_25 = {row.g_25}'
    )
    sql_execute(conn, query)
    return query
tier=['IRON','BRONZE','SILVER','GOLD','PLATINUM','EMERAID','DIAMOND','MASTER','GRANDMASTER']
for i in range(1000):
    for t in tier:
        try:
            raw_data=get_rawdata(t)
            df=get_match_timeline_df(raw_data)
            conn=connect_mysql()
            df.progress_apply(lambda x:insert_matches_timelines_mysql(x,conn),axis=1)
            conn.commit()
            conn.close()
            print(f'semi의 {i}번째 완료')
            time.sleep(90)
        except Exception as e:
            print(f'{e}의 원인으로 insert 실패')
            time.sleep(90)


print('import complet')
