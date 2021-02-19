'''
========================================
EXTRACT and PROCESS Azure AD Users
========================================

# Author: @bencarpena

# Workflow:
-   Generate token for access to Graph API
-   Run Graph API Query
-   Read httpclient output
-   Convert AAD OData JSON to tab-delimited TXT file
-   Convert tab-delimited TXT file to JSON then to PARQUET (in-progress)
-   File will then be loaded to Data Lake and Azure Data Factory for further processing (in-progress)


# Change log:
@bencarpena     :   20210131 : 	initial codes created; v1
                :   20210201 :  added scaffold and strawman
                :   20210206 :  added txt --> json converter; upgraded to use global variables
                             :  beautified filename; PEP8 naming convention
                             :  added subroutines to get 'nextPage' API results
                :   20210214 :  Added _sysparams for json output flag

'''

import sys
import http.client
import json 
import csv 
from datetime import datetime
import pandas as pd
import ssl


try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context



# === Display Program Execution start time ====
print ("### Python program START ### : " + str(datetime.now()))


# ========== Instantiate "environment" variables & containers ==========
# Write Container list
global writeList
writeList = ['id', 'Display Name', 'Given Name', 'Surname', 'Email', 'Job Title', \
    'CAI', 'Provisioning ID', 'HR GUID', \
    'PersonRelationshipType', \
    'Level 1 Name', 'Level 1 Code', \
    'Level 2 Name', 'Level 2 Code', \
    'Level 3 Name', 'Level 3 Code', \
    'Level 4 Name', 'Level 4 Code', \
    'Level 5 Name', 'Level 5 Code', \
        ]
global fields
fields = writeList

global _sysparams
_sysparams = False 


# HELIOS
payload_path = '/Users/bencarpena/OneDrive/_Projects/Projects/targetprocess/files/graph/'




# ============================== ===
# Process JSON payload
# ============================== ===

# --- Address data quality issues at graph; strip None ---
# Graph sometimes returns `None`
def bstr(_string):
    if _string is None:
        return 'None'
    return str(_string)

def process_GraphAPI_Results(_payload):
    global writeList 
    #===== Open file and transform + enrich =====

    # === CONVERT str to json ===
    data = json.loads(_payload) 
    #debug only: print (data)


    # ====== WRITE output file headers =========
    write_to_txt_file(writeList)

    # ==== READ Payload =====
    data_collection = data['value']
    #debug only: print(data_collection)


    # ==== PARSE and PROCESS Graph API Payload =====
    i=0
    for rows in data_collection:
        writeList = []
        d0 = data['value'][i]
        #debug only: print(d0) #dictionary; key-value pair
        
        # +++++ optional: parse through fields +++++

        # +++ parse method 1: +++
        '''
        for d_inner_contents in enumerate(d0.items()): 
            print (d_inner_contents) 
        '''

        # +++ parse method 2: +++
        '''
        for d_key, d_value in d0.items(): 
            print (d_key, d_value)
        '''

        # ======= READ & GET only needed Dictionary key-value contents =======
        aad_id = bstr(d0['id'])
        displayName = bstr(d0['displayName'])
        givenName = bstr(d0['givenName'])
        surname = bstr(d0['surname'])
        email = bstr(d0['userPrincipalName'])
        jobTitle = bstr(d0['jobTitle']) 

        

        # --- Handle data inconsistencies in output ---
        if 'extension_39c7d3e68666465dab296ee0fc538118_cvx_ProvisioningID' in data['value'][i]:
            ProvisioningID = bstr(d0['extension_39c7d3e68666465dab296ee0fc538118_cvx_ProvisioningID'])
        else:
            ProvisioningID = 'None'

        if 'extension_39c7d3e68666465dab296ee0fc538118_extensionAttribute11' in data['value'][i]:
            CAI = bstr(d0['extension_39c7d3e68666465dab296ee0fc538118_extensionAttribute11'])
        else:
            CAI = 'None'
        
        if 'extension_39c7d3e68666465dab296ee0fc538118_cvx_HRGUID' in data['value'][i]:
            HrGUID = bstr(d0['extension_39c7d3e68666465dab296ee0fc538118_cvx_HRGUID'])
        else:
            HrGUID = 'None'
        
        if 'extension_39c7d3e68666465dab296ee0fc538118_cvx_PersonRelationshipType' in data['value'][i]:
            personRType = bstr(d0['extension_39c7d3e68666465dab296ee0fc538118_cvx_PersonRelationshipType'])
        else:
            personRType = 'None'



        if 'chevron_organization' in data['value'][i]:
            L5name = bstr(d0['chevron_organization']['level5Name'])
            L5code = bstr(d0['chevron_organization']['level5Code'])
            L4name = bstr(d0['chevron_organization']['level4Name'])
            L4code = bstr(d0['chevron_organization']['level4Code'])
            L3name = bstr(d0['chevron_organization']['level3Name'])
            L3code = bstr(d0['chevron_organization']['level3Code'])
            L2name = bstr(d0['chevron_organization']['level2Name'])
            L2code = bstr(d0['chevron_organization']['level2Code'])
            L1name = bstr(d0['chevron_organization']['level1Name'])
            L1code = bstr(d0['chevron_organization']['level1Code'])
        else:
            L5name = 'None'
            L5code = 'None'
            L4name = 'None'
            L4code = 'None'
            L3name = 'None'
            L3code = 'None'
            L2name = 'None'
            L2code = 'None'
            L1name = 'None'
            L1code = 'None'

        # ==== Store needed data in list ===
        writeList.append(aad_id.replace('\t', ''))
        writeList.append(displayName.replace('\t', ''))
        writeList.append(givenName.replace('\t', ''))
        writeList.append(surname.replace('\t', ''))
        writeList.append(email.replace('\t', ''))
        writeList.append(jobTitle.replace('\t', ''))
        writeList.append(CAI.replace('\t', ''))
        writeList.append(ProvisioningID.replace('\t', ''))
        writeList.append(HrGUID.replace('\t', ''))
        writeList.append(personRType.replace('\t', ''))
        writeList.append(bstr(L1name).replace('\t', ''))
        writeList.append(bstr(L1code).replace('\t', ''))
        writeList.append(bstr(L2name).replace('\t', ''))
        writeList.append(bstr(L2code).replace('\t', ''))
        writeList.append(bstr(L3name).replace('\t', ''))
        writeList.append(bstr(L3code).replace('\t', ''))
        writeList.append(bstr(L4name).replace('\t', ''))
        writeList.append(bstr(L4code).replace('\t', ''))
        writeList.append(bstr(L5name).replace('\t', ''))
        writeList.append(bstr(L5code).replace('\t', ''))

        # ====== WRITE to file =========
        write_to_txt_file(writeList)

        i+= 1

# ============================== ===
# CREATE Tab-delimited TXT File
# ============================== ===

def write_to_txt_file(_i_List):
    _list_length = len(_i_List)
    with open(payload_path + 'graphapi_users_' + output_file_stamp + '.txt', 'a') as file_output:
        for _i_key, _i_val in enumerate(_i_List):
            if _i_key < _list_length -1:
                file_output.write(str(_i_val) + '\t')
            else:
                file_output.write(str(_i_val))
        file_output.write('\n')

  
# ============================== ===
# Create Timestamp
# ============================== ===
def create_timestamp():
    #datetime_object = datetime.strptime('Jun 1 2020  1:33PM', '%b %d %Y %I:%M%p')
    #print (datetime_object)
    dt = str(datetime.fromisoformat(str(datetime.now())))
    dt = dt.replace('-', '')
    dt = dt.replace(' ', '_')
    dt = dt.replace(':','')
    dt = dt.replace('.', '_')
    return dt


# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ ===>
# Code Driver : MAIN
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ ===>
try:

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
    conn = http.client.HTTPSConnection("login.microsoftonline.com")
    payload = 'client_id=___&client_secret=___&grant_type=client_credentials&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default'
    headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': 'x-ms-gateway-slice=prod; stsservicecookie=ests; fpc=AtFNoyrjgQpIlyC0MQvqfvYi7P8JAQAAAI6uptcOAAAA'
    }
    conn.request("GET", "/fd799da1-bfc1-4234-a91c-72b3a1cb9e26/oauth2/v2.0/token", payload, headers)
    res = conn.getresponse()
    token_ = res.read().decode("utf-8")


    # ===== CONVERT response to JSON =====
    graph_aad_dict = json.loads(token_)



    # ==== READ access token from dictionary =====
    access_token = graph_aad_dict['access_token']


    # ==== EXECUTE Graph API ====
    conn = http.client.HTTPSConnection("graph.microsoft.com")
    headers = { 'Authorization': 'Bearer ' + access_token}

    # ==== FORM initial Graph Request ====
    # prod graph call ----- with specifics and extended attributes
    '''
    _graphreq = '/v1.0/users?$select=id,accountEnabled,userPrincipalName,\
displayName,surname,givenName,jobTitle,extension_39c7d3e68666465dab296ee0fc538118_cvx_HRGUID,\
extension_39c7d3e68666465dab296ee0fc538118_cvx_ProvisioningID,\
extension_39c7d3e68666465dab296ee0fc538118_cvx_SourcingCompanyID,\
extension_39c7d3e68666465dab296ee0fc538118_extensionAttribute11,\
extension_39c7d3e68666465dab296ee0fc538118_cvx_PersonRelationshipType,\
manager,state,country,mobilephone,chevron_organization'
    '''


    # Filtered Graph request (demo)
    
    _graphreq = "/v1.0/users?$filter=startswith(displayName,\
'benjamin')&$select=id,accountEnabled,userPrincipalName,\
displayName,surname,givenName,jobTitle,extension_39c7d3e68666465dab296ee0fc538118_cvx_HRGUID,\
extension_39c7d3e68666465dab296ee0fc538118_cvx_ProvisioningID,\
extension_39c7d3e68666465dab296ee0fc538118_cvx_SourcingCompanyID,\
extension_39c7d3e68666465dab296ee0fc538118_extensionAttribute11,\
extension_39c7d3e68666465dab296ee0fc538118_cvx_PersonRelationshipType,\
manager,state,country,mobilephone,chevron_organization"
    
    conn.request("GET", _graphreq, payload, headers)

    # ==== READ returned API payload ====
    res = conn.getresponse()
    Graph_Data = res.read().decode("utf-8")
    #print(_graph_data)


    #3 Process and Generate Output
    # -----------------------------------------
    while nextPageExists == True:
        process_GraphAPI_Results(Graph_Data)

        conn = http.client.HTTPSConnection("graph.microsoft.com")
        headers = {
        'Authorization': 'Bearer ' + access_token
        }

        # === CHECK if next page exists ===
        nextPage = ''
        Graph_Data_json = json.loads(Graph_Data)
        if '@odata.nextLink' in Graph_Data_json:
            nextPage = Graph_Data_json['@odata.nextLink']
            # format nextPage
            nextPage = '/' + nextPage.strip('https://graph.microsoft.com')
            nextPageExists = True
            #debug only: print("Next page: ", nextPage)
        else:
            nextPage = ''
            nextPageExists = False
            break


        # ==== GO to next API result set  ====
        _graphreq = nextPage

        conn.request("GET", _graphreq, payload, headers)

        # ==== READ returned API payload ====
        res = conn.getresponse()
        Graph_Data = res.read().decode("utf-8")
        #debug only: print (Graph_Data)


    # 20210213 : Need to parameterize json converter subroutines; file size could reach GB if full pull is done
    if _sysparams == True:
        #4 Convert output file from txt --> json
        # -----------------------------------------
        output_file_txt = payload_path + 'graphapi_users_' + output_file_stamp + '.txt'
        
        # MAIN container dictionary 
        dict1 = {}
        
        with open(output_file_txt) as fh: 
            # count variable for azure ad user id creation 
            ctr_entry = 0
            for line in fh: 
                if line == '':
                    break
                else:
                    definitions = list( line.strip().split("\t", 20))
                    # for automatic creation of id for each Azure AD User 
                    if ctr_entry == 0:
                        aad_no = 'header'
                    else:
                        aad_no ='detail_aad_rec_'+str(ctr_entry) 
                
                    # loop variable 
                    i = 0
                    # intermediate dictionary 
                    dict2 = {} 
                    while i<len(fields): 
                            # creating dictionary for each Azure AD User 
                            dict2[fields[i]]= definitions[i] 
                            i+=1
                            
                    # append the record of each Azure AD User to the main dictionary 
                    dict1[aad_no]= dict2 
                    ctr_entry += 1
        
        
        # creating json file         
        out_file_json = open(payload_path + 'graphapi_users_' + output_file_stamp + '.json', "w") 
        json.dump(dict1, out_file_json, indent = 4) 
        out_file_json.close() 
    

    #4 Write output file to parquet (PAUSED)
    # -----------------------------------------
    #   ---- Convert output file from json --> parquet

    #   ---- Read file via pandas 

except:
    err = sys.exc_info()[0]
    print (err)    
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
