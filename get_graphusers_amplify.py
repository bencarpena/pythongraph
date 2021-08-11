'''
========================================
EXTRACT and PROCESS Graph json data
========================================

# Author: @bencarpena

# Workflow:
[+]   Generate token for access to Graph API
[+]   Run Graph API Query
[+]   Read httpclient output
[+]   Create Lego : Unpack + Flatten; Convert AAD OData JSON to tab-delimited TXT file

# Change log:
@bencarpena     :   20210131 : 	initial codes created; v1
                :   20210201 :  added scaffold and strawman
                :   20210212 :  added bstr function to address data quality issues with Graph; `None`
                :   20210213 :  Need to parameterize json converter subroutines; 
                             :  File size could reach GB if full pull is done; memory intensive
                :   20210625 :  upgraded and enhanced bstr function


'''

import sys
import http.client
import json 
import csv 
from datetime import datetime
import pandas as pd
import ssl
import requests
import lihim
import re


def post_slack_msg(_message):
    ########### Post Log at Slack Channel #viewbag ################
    dtstamp = datetime.now()
    slack_msg = {'text' : '#Graph (Azure AD : fd799da1-bfc1-4234-a91c-72b3a1cb9e26) | ' + str(dtstamp) + ' | ' + _message}
    webhook_url = lihim.slack_webhook_viewbag #viewbag 
    requests.post(webhook_url, data=json.dumps(slack_msg))


# ======================================================= ===
# STRINGIFY payload
#   Notes:
#   - Graph sometimes returns `None`
#   - Address data quality issues at graph; strip None
# ======================================================= ===

def bstr(_string):
    if _string is None:
        return 'None'
    else:
        regex = re.compile(r'[\n\r\t]')
        return regex.sub('', str(_string))

# ============================== ===
# CREATE Tab-delimited TXT File
# ============================== ===
def write_to_txt_file(_i_List, _fileout):
    _list_length = len(_i_List)
    with open(payload_path + _fileout, 'a') as file_output:
        for _i_key, _i_val in enumerate(_i_List):
            if _i_key < _list_length -1:
                file_output.write(str(_i_val) + '\t')
            else:
                file_output.write(str(_i_val))
        file_output.write('\n')

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context

# ============================== ===
# GENERATE access token
# ============================== ===
def get_access_token(_clientid):
    # ==== GENERATE Access Token ====
    conn = http.client.HTTPSConnection("login.microsoftonline.com")
    payload = 'client_id=' + lihim.graph_client_id + '&client_secret=' + lihim.graph_secret + '&grant_type=client_credentials&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default'
    headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
    }
    conn.request("GET", "/your_tenant_id/oauth2/v2.0/token", payload, headers)
    res = conn.getresponse()
    token_ = res.read().decode("utf-8")
    return token_


# ======================================================================== ===
# PROCESS graph json output  
#   - $$$ Iterate through nested sources dictionary{} and list[] $$$
#   - Bread and butter
# ======================================================================== ===

def get_list_values(_list, _key, _fileout):
    _itemctr = 1
    for _items in _list:
        if type(_items) is dict:
            get_webster_values(_items, _fileout, False)
        else:
            #print(_items) #debug only
            writeList = []
            writeList.append(aad_id.replace('\t', '')) #key 1
            _write_key = _key + '_' + str(_itemctr) # key 2
            _write_item = bstr(_items) # value
            writeList.append(_write_key) 
            writeList.append(_write_item.replace('\t', '')) 
            write_to_txt_file(writeList, _fileout)
        _itemctr += 1

def get_webster_values(_payload, _fileout, _blnwebster):
    global _key_hdr_dict

    for _key, _value in _payload.items():
        if type(_value) is dict:
            # --- get additional value header ---
            _key_hdr_dict = bstr(_key)
            get_webster_values(_value, _fileout, True)
        elif type(_value) is list:
            # --- get additional value header --- 
            _key_hdr_list = bstr(_key)
            get_list_values(_value, _key, _fileout)
        else:
            if _blnwebster == True:
                writeList = []
                writeList.append(aad_id.replace('\t', '')) #key 1
                _write_key =  _key_hdr_dict + '.' + _key # key 2
                _write_item = bstr(_value) # value
                writeList.append(_write_key) 
                writeList.append(_write_item.replace('\t', '')) 
                write_to_txt_file(writeList, _fileout)
            else:
                writeList = []
                writeList.append(aad_id.replace('\t', '')) #key 1
                _write_key = _key # key 2
                _write_item = bstr(_value) # value
                writeList.append(_write_key) 
                writeList.append(_write_item.replace('\t', '')) 
                write_to_txt_file(writeList, _fileout)

def process_graphapi_results(_payload, _fileidentifier, _accesstoken):
    global writeList
    global aad_id
 
    # === CONVERT str to json ===
    data = json.loads(_payload) 

    # ==== READ Payload =====
    data_collection = data['value']

    # ==== PARSE and PROCESS Graph API Payload =====
    i=0
    for rows in data_collection:
        writeList = []
        d0 = data['value'][i]
        
        # ======= READ & GET only needed Dictionary key-value contents =======
        aad_id = bstr(d0['id'])
        get_webster_values(d0, 'graphapi_identities_' + output_file_stamp + '.txt', False)
        i+= 1

    post_slack_msg("[INFO] | processed " + str(i) + " records in this graph api dataset/batch")


# ============================== ===
# Create Timestamp
# ============================== ===
def create_timestamp():
    dt = str(datetime.fromisoformat(str(datetime.now())))
    dt = dt.replace('-', '')
    dt = dt.replace(' ', '_')
    dt = dt.replace(':','')
    dt = dt.replace('.', '_')
    return dt


# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ ===#
# Code Driver : MAIN
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ ===#
try:
    
    # ========== Instantiate "environment" variables & containers ==========
    # Write Identity list
    global writeList
    writeList = ['AAD_ID', 'GRAPH_KEY', 'GRAPH_VALUE']

    global fields
    fields = writeList

    global _sysparams
    _sysparams = False 

    # === Display Program Execution start time ====
    print ("### Python program START ### : " + str(datetime.now()))
    #debug only
    post_slack_msg("[INFO] | ###$ python program start $###")

    payload_path = '/local_path_here/'

    nextPageExists = True

    #0 Just for fun : Einstein Easter Egg
    # -----------------------------------------
    program_args = {'emc2' : 'Einstein Easter Egg',
    'pray' : 'Generate JSON file output'
    }
    if len(sys.argv) == 2 and sys.argv[1] in program_args:
        
        if sys.argv[1] == 'emc2': # Force an error
            homer_equation = 1000000 / 0
            sys.exit(1)
        elif sys.argv[1] == 'pray':
            #global _sysparams 
            _sysparams = True 


    #1 Create unique file index for output
    # -----------------------------------------
    output_file_stamp = create_timestamp()
    

    #2 Connect to Graph and Get payload
    # -----------------------------------------
    # ==== GENERATE Access Token ====
    token_ = get_access_token('[1] : dfad8261-1234-5678-a722-000000')
    # ===== CONVERT response to JSON =====
    graph_aad_dict = json.loads(token_)
    # ==== READ access token from dictionary =====
    access_token = graph_aad_dict['access_token']

    #debug only : post at Slack
    post_slack_msg("[1] | access token generated")


    # ==== EXECUTE Graph API ====
    conn = http.client.HTTPSConnection("graph.microsoft.com")
    headers = { 'Authorization': 'Bearer ' + access_token}
    payload = 'client_id=' + lihim.graph_client_id + '&client_secret=' + lihim.graph_secret + '&grant_type=client_credentials&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default'


    # ==== FORM initial Graph Request ====
    # prod graph call ----- with specifics and extended attributes
    
    _graphreq = '/v1.0/users?$select=id,accountEnabled,userPrincipalName,\
displayName,surname,givenName,jobTitle,\
manager,city,state,country,mobilephone,officeLocation,mail&$top=900'
    
    conn.request("GET", _graphreq, payload, headers)

    # ====== WRITE output file headers =========
    #1  identities
    write_to_txt_file(writeList, 'graphapi_identities_' + output_file_stamp + '.txt')

    # ==== READ returned API payload ====
    res = conn.getresponse()
    Graph_Data = res.read().decode("utf-8")
    #print(_graph_data) #debug only


    #3 Process + Unpack + Flatten data and Generate Output
    # ------------------------------------------------------
    _ctr = 1
    while nextPageExists == True:
        process_graphapi_results(Graph_Data, output_file_stamp, access_token)
        _ctr +=1

        # ==== GENERATE Access Token ====
        token_ = get_access_token(lihim.graph_client_id)
        # ===== CONVERT response to JSON =====
        graph_aad_dict = json.loads(token_)
        # ==== READ access token from dictionary =====
        access_token = graph_aad_dict['access_token']

        #debug only
        print ("$ access token generated : " + str(datetime.now()))
        #debug only : post at Slack
        post_slack_msg("[" + str(_ctr) +  "] | access token generated")

        conn = http.client.HTTPSConnection("graph.microsoft.com")
        headers = {
        'Authorization': 'Bearer ' + access_token
        }

        # === CHECK if next page exists ===
        nextPage = ''
        Graph_Data_json = json.loads(Graph_Data) #str to json
        if '@odata.nextLink' in Graph_Data_json:
            nextPage = Graph_Data_json['@odata.nextLink']
            # format nextPage
            nextPage = '/' + nextPage.strip('https://graph.microsoft.com')
            nextPageExists = True
        else:
            nextPage = ''
            nextPageExists = False
            break


        # ==== GO to next API result set  ====
        _graphreq = nextPage
        conn.request("GET", _graphreq, payload, headers)

        # ==== READ returned API payload ====
        res = conn.getresponse()
        
        try:
            blnConnErr = False 
            Graph_Data = res.read().decode("utf-8", errors='ignore')
        except ConnectionResetError:
            post_slack_msg("[ERROR] | ConnectionResetError at retry (0). Sent request [" + str(_graphreq) +  "] ")
            blnConnErr = True 
            continue 

        _retry = 1
        while blnConnErr == True:
            if _retry >= 5:
                post_slack_msg("[SELF-TERMINATE] | Terminating execution quit() at retry (" + str(_retry) + "). Sent request [" + str(_graphreq) +  "] ")
                quit()
            else:
                try:
                    res = conn.getresponse()
                    Graph_Data = res.read().decode("utf-8", errors='ignore')
                    blnConnErr = False
                except:
                    post_slack_msg("[SELF-HEAL] | ConnectionResetError at retry (" + str(_retry) + "). Sent request [" + str(_graphreq) +  "] ")
                    blnConnErr = True 
                    continue
            _retry += 1


except:
    err = sys.exc_info()[0]
    print (str(datetime.now()) + ' : ' + str(err))     
    post_slack_msg("[ERROR] | " + str(err) + " | ###$ python program terminated $###")
    print ('''
                        .+~                :xx++::
                    :`. -          .!!X!~"?!`~!~!. :-:.
                    {             .!!!H":.~ ::+!~~!!!~ `%X.
                    '             ~~!M!!>!!X?!!!!!!!!!!...!~.
                                {!:!MM!~:XM!!!!!!.:!..~ !.  `{
                    {: `   :~ .:{~!!M!XXHM!!!X!XXHtMMHHHX!  ~ ~
                    ~~~~{' ~!!!:!!!!!XM!!M!!!XHMMMRMSXXX!!!!!!:  {`
                    `{  {::!!!!!X!X?M!!M!!XMMMMXXMMMM??!!!!!?!:~{
                : '~~~{!!!XMMH!!XMXMXHHXXXXM!!!!MMMMSXXXX!!!!!!!~
                :    ::`~!!!MMMMXXXtMMMMMMMMMMMHX!!!!!!HMMMMMX!!!!!: ~
                '~:~!!!!!MMMMMMMMMMMMMMMMMMMMMMXXX!!!M??MMMM!!X!!i:
                {~{!!!!!XMMMMMMMMMMMM8M8MMMMM8MMMMMXX!!!!!!!!X!?t?!:
                ~:~~!!!!?MMMMMM@M@RMRRR$@@MMRMRMMMMMMXSX!!!XMMMX{?X!
                :XX {!!XHMMMM88MM88BR$M$$$$8@8RN88MMMMMMMMHXX?MMMMMX!!!
            .:X! {XMSM8M@@$$$$$$$$$$$$$$$$$$$B8R$8MMMMMMMMMMMMMMMMX!X
            :!?! !?XMMMMM8$$$$8$$$$$$$$$$$$$$BBR$$MMM@MMMMMMMMMMMMMM!!X
            ~{!!~ {!!XMMMB$$$$$$$$$$$$$$$$$$$$$$$$MMR$8MR$MMMMMMMMMMMMM!?!:
            :~~~ !:X!XMM8$$$$$$$$$$$$$$$$$$$$$$$RR$$MMMMR8NMMMMMMMMMMMMM{!`-
        ~:{!:~`~':!:HMM8N$$$$$$$$$$$$$$$$$$$$$$$$$8MRMM8R$MRMMMMMMMMRMMMX!
    !X!``~~   :~XM?SMM$B$$$$$$$$$$$$$$$$$$$$$$BR$$MMM$@R$M$MMMMMM$MMMMX?L
    X~.      : `!!!MM#$RR$$$$$$$$$$$$$$$$$R$$$$$R$M$MMRRRM8MMMMMMM$$MMMM!?:
    ! ~ {~  !! !!~`` :!!MR$$$$$$$$$$RMM!?!??RR?#R8$M$MMMRM$RMMMM8MM$MMM!M!:>
    : ' >!~ '!!  !   .!XMM8$$$$$@$$$R888HMM!!XXHWX$8$RM$MR5$8MMMMR$$@MMM!!!{ ~
    !  ' !  ~!! :!:XXHXMMMR$$$$$$$$$$$$$$$$8$$$$8$$$MMR$M$$$MMMMMM$$$MMM!!!!
    ~{!!!  !!! !!HMMMMMMMM$$$$$$$$$$$$$$$$$$$$$$$$$$MMM$M$$MM8MMMR$$MMXX!!!!/:`
    ~!!!  !!! !XMMMMMMMMMMR$$$$$$$$$$$$R$RRR$$$$$$$MMMM$RM$MM8MM$$$M8MMMX!!!!:
    !~ ~  !!~ XMMM%!!!XMMX?M$$$$$$$$B$MMSXXXH?MR$$8MMMM$$@$8$M$B$$$$B$MMMX!!!!
    ~!    !! 'XMM?~~!!!MMMX!M$$$$$$MRMMM?!%MMMH!R$MMMMMM$$$MM$8$$$$$$MR@M!!!!!
    {>    !!  !Mf x@#"~!t?M~!$$$$$RMMM?Xb@!~`??MS$M@MMM@RMRMMM$$$$$$RMMMMM!!!!
    !    '!~ {!!:!?M   !@!M{XM$$R5M$8MMM$! -XXXMMRMBMMM$RMMM@$R$BR$MMMMX??!X!!
    !    '!  !!X!!!?::xH!HM:MM$RM8M$RHMMMX...XMMMMM$RMMRRMMMMMMM8MMMMMMMMX!!X!
    !     ~  !!?:::!!!MXMR~!MMMRMM8MMMMMS!!M?XXMMMMM$$M$M$RMMMM8$RMMMMMMMM%X!!
    ~     ~  !~~X!!XHMMM?~ XM$MMMMRMMMMMM@MMMMMMMMMM$8@MMMMMMMMRMMMMM?!MMM%HX!
            !!!!XSMMXXMM .MMMMMMMM$$$BB8MMM@MMMMMMMR$RMMMMMMMMMMMMMMMXX!?H!XX
            XHXMMMMMMMM!.XMMMMMMMMMR$$$8M$$$$$M@88MMMMMMMMMMMMMMM!XMMMXX!!!XM
        ~   {!MMMMMMMMRM:XMMMMMMMMMM8R$$$$$$$$$$$$$$$NMMMMMMMM?!MM!M8MXX!!/t!M
        '   ~HMMMMMMMMM~!MM8@8MMM!MM$$8$$$$$$$$$$$$$$8MMMMMMM!!XMMMM$8MR!MX!MM
            'MMMMMMMMMM'MM$$$$$MMXMXM$$$$$$$$$$$$$$$$RMMMMMMM!!MMM$$$$MMMMM{!M
            'MMMMMMMMM!'MM$$$$$RMMMMMM$$$$$$$$$$$$$$$MMM!MMMX!!MM$$$$$M$$M$M!M
            !MMMMMM$M! !MR$$$RMM8$8MXM8$$$$$$$$$$$$NMMM!MMM!!!?MRR$$RXM$$MR!M
            !M?XMM$$M.{ !MMMMMMSUSRMXM$8R$$$$$$$$$$#$MM!MMM!X!t8$M$MMMHMRMMX$
        ,-,   '!!!MM$RMSMX:.?!XMHRR$RM88$$$8M$$$$$R$$$$8MM!MMXMH!M$$RMMMMRNMMX!$
    -'`    '!!!MMMMMMMMMM8$RMM8MBMRRMR8RMMM$$$$8$8$$$MMXMMMMM!MR$MM!M?MMMMMM$
            'XX!MMMMMMM@RMM$MM@$$BM$$$M8MMMMR$$$$@$$$$MM!MMMMXX$MRM!XH!!??XMMM
            `!!!M?MHMMM$RMMMR@$$$$MR@MMMM8MMMM$$$$$$$WMM!MMMM!M$RMM!!.MM!%M?~!
            !!!!!!MMMMBMM$$RRMMMR8MMMMMRMMMMM8$$$$$$$MM?MMMM!f#RM~    `~!!!~!
            ~!!HX!!~!?MM?MMM??MM?MMMMMMMMMRMMMM$$$$$MMM!MMMM!!
            '!!!MX!:`~~`~~!~~!!!!XM!!!?!?MMMM8$$$$$MMMMXMMM!!
                !!~M@MX.. {!!X!!!!XHMHX!!``!XMMMB$MM$$B$M!MMM!!
                !!!?MRMM!:!XHMHMMMMMMMM!  X!SMMX$$MM$$$RMXMMM~
                !M!MMMM>!XMMMMMMMMXMM!!:!MM$MMMBRM$$$$8MMMM~
                `?H!M$R>'MMMM?MMM!MM6!X!XM$$$MM$MM$$$$MX$f
                `MXM$8X MMMMMMM!!MM!!!!XM$$$MM$MM$$$RX@"
                ~M?$MM !MMMMXM!!MM!!!XMMM$$$8$XM$$RM!`
                    !XMMM !MMMMXX!XM!!!HMMMM$$$$RH$$M!~
                    'M?MM `?MMXMM!XM!XMMMMM$$$$$RM$$#
                    `>MMk ~MMHM!XM!XMMM$$$$$$BRM$M"
                    ~`?M. !M?MXM!X$$@M$$$$$$RMM#
                        `!M  !!MM!X8$$$RM$$$$MM#`
                        !% `~~~X8$$$$8M$$RR#`
                        !!x:xH$$$$$$$R$R*`
                            ~!?MMMMRRRM@M#`       
                            `~???MMM?M"`
                                ``~~

    # @bencarpena : #AlwaysDay1

    '''
    )
finally:
    print('''
#Codes and #Design by @bencarpena
     _____    _____    
    |  __ \  /     \  
    |  __ <  |  <--<  
    |_____/  \_____/ 

    '''
    )
    # === Display Program Execution end time ====
    print ("### Python program END ### : " + str(datetime.now()))
    #debug only
    post_slack_msg("[INFO] | ###$ python program end $###")

