#!/usr/bin/python3 -u
# encoding=utf-8
# erzeugt einen lokalen Webserver der Daten von einer Wetterstation entgegen nimmt und die Werte per UDP an Loxone oder
# andere Ziele (auch per Broadcast) schickt; optional verschiedene Export- und Weiterleitungsmoeglichkeiten wie WU, CSV,
# W4L, JSON, HTML
# eingehende UDP-Befehle (reboot, setWSconfig) werden umgewandelt und per UDP an die Wetterstation versandt
# moegliche Startparameter:
# -help -getWSIP, -getWSPORT, -getCSVHEADER, -createConfig, -autoConfig -patchW4L -recoverW4L -setWSInterval
# -setWSconfig, -checkLBUPort, -checkLBHPort -getWSINTERVAL (mit Config-File)
#
# Sonnenscheindauer: basiert auf Code von Jterrettaz/sunduration https://github.com/Jterrettaz/sunduration und Werner Krenn
#
# Oliver Engel; 15.12.19, 28.12.19, 18.01.20, 20.02.20, 26.04.20, 20.07.20, 25.11.20, 19.02.21, 27.06.21, 22.01.22
# FOSHKplugin@phantasoft.de - http://foshkplugin.phantasoft.de

try:
  import sys
  import math
  from math import sin,cos,pi,asin		##
  from http.server import HTTPServer, BaseHTTPRequestHandler
  import json
  import socket
  import logging
  import requests
  import time
  import logging.handlers
  import configparser
  import os
  import subprocess
  import hashlib
  from os import path
  from collections import deque
  import pickle
  import signal
  import threading
  from threading import Timer
  import ftplib
  import io
  import paho.mqtt.publish as publish
  from influxdb import InfluxDBClient
except ImportError as error:
  errstr = str(error).replace("'","")
  print("import failed: "+errstr+"\n")
  exit(1)

# adjust here if necessary (but defaults to starting dir or LB-Plugin-dir
CONFIG_FILE = ""
#CONFIG_FILE = "/root/foshkplugin.conf"

prgname = "FOSHKplugin"
prgver = "v0.09"

myDebug = False                                  # set to True to enable Debug-messages

defSID = "FOSHKweather"                          # default SensorID SID for outgoing UDP-datagrams
maxfwd = 99                                      # max. Anzahl der zusaetzlichen Forwards
w4l_feldanzahl = 41                              # Anzahl der Felder in der current.dat von Weather4Loxone
httpTimeOut = 8                                  # Timeout in Sekunden fuer sendendes GET/POST
httpTries = 3                                    # count of tries for http-connect
httpSleepTime = 6                                # time between http send attempts
udpTimeOut = 3                                   # Timeout in Sekunden fuer sockets
execTimeOut = 15                                 # Timeout in seconds for executing external scripts
LOG_LEVEL = "ALL"                                # specify the default log level (ERROR, WARNING, INFO, ALL)

cmd_discover     = "\xff\xff\x12\x00\x04\x16"
cmd_reboot       = "\xff\xff\x40\x03\x43"
cmd_get_customC  = "\xff\xff\x51\x03\x54"        # ff ff 51 03 54 (last byte: CRC)
cmd_get_customE  = "\xff\xff\x2a\x03\x2d"        # ff ff 2a 03 2d (last byte: CRC)
cmd_set_customC  = "\xff\xff\x52"                # ff ff 52 Len [Laenge Path Ecowitt][Path Ecowitt][Laenge Path WU][Path WU][crc]
cmd_set_customE  = "\xff\xff\x2b"                # ff ff 2b Len [Laenge ID][ID][Laenge Key][Key][Laenge IP][IP][Port][Intervall][ecowitt][enable][crc]
cmd_get_FWver    = "\xff\xff\x50\x03\x53"        # ff ff 50 03 53 (last byte: CRC)
cmd_get_MAC      = "\xff\xff\x26\x03\x29"
ok_set_customE   = "\xff\xff\x2b\x04\x00\x2f"
ok_set_customC   = "\xff\xff\x52\x04\x00\x56"
ok_cmd_reboot    = "\xff\xff\x40\x04\x00\x44"

# global um von ueberall darauf zugreifen zu koennen
last_RAWstr = ""
last_csv_time = 0
last_ws_time = 0
last_d_m = {}                                    # list with all key/value pairs as metric values
last_d_e = {}                                    # list with all key/value pairs as imperial values
last_d_all = {}                                  # list with all known key/value pairs
last_FWver = ""                                  # firmware version of sending weather station
inWStimeoutWarning = False
inStormWarning = False
inStorm3h = False
inSensorWarning = False
inStormWarnStart = 0
inTSWarning = False
inTSWarnStart = 0
last_lightning_time = 0                          # set 0 as default for last_lightning_time (will be set from config-file)
last_lightning = 0                               # set 0 as default for last_lightning (will be set from config-file)
inTS_lightning_num = 0                           # set inTS_lightning_num to 0
ldmin = 0                                        # min ligthning distance
ldmax=0                                          # max ligthning distance
ldsum=0                                          # sum ligthning distance (for average)
inBatteryWarning = False
preSensorWarning = False
inLeakageWarning = False
inCO2Warning = False                             # current state of CO2 warning
updateWarning = False
last_hpaTrend1h = 0
last_hpaTrend3h = 0
OutEncoding = "ISO-8859-1"
exchangeTime = False                             # set incoming time to time of receiving if True
PO_ENABLE = False                                # enable/disable Pushover
LOG_IGNORE = []                                  # a list with substrings to not write to logfile
# v0.08 MQTT
last_mqtt = {}                                   # last dict sent by MQTT
MQTTsendTime = 0                                 # last time MQTT was sent
# v0.08 resend status via UDP
UDP_STATRESEND_time = 0                          # last time the status was sent by UDP
# v0.08 WSWin-Forward: CSV-Header
WSWinCSVHeader = ";;1;17;133;2;18;35;36;45;134;42;41;3;19;4;20;5;21;6;22;7;23;8;24;29;30;31;32;25;26;27;28;37;13;14;15;16\r\n"
# v0.09: set some more defaults
AUTH_PWD = ""                                    # default if config-file is missinf
loglog = ""                                      # default if config-file is missinf
execOnly = "EXECONLY"                            # just exec the script but not send the data
# v0.09: count real interval time
lastData = 0                                     # save time of last incoming data transmission
intervald = deque(maxlen=(10))                   # last n intervals
inIntervalWarning = False                        # in warning state

def doNothing():
  return

def hidePASSKEY(s):
  if AUTH_PWD != "": s = s.replace(AUTH_PWD,"[PASSKEY]")
  return s

def mkBoolean(s):
  true = ["TRUE","YES","ENABLE","ON","1"]
  return True if str(s).upper() in true else False

def strToNum(s):
  if type(s) == str:
    try:
      s = int(s) if not "." in s else float(s)
    except: pass
  return s

def readConfigFile(configname):
  # v0.08 ignore duplicate sections (will be ignored while starting and deleted on exit
  config = configparser.RawConfigParser(inline_comment_prefixes='#',strict=False)
  config.optionxform = str
  try:
    config.read(configname, encoding='ISO-8859-1')
  except configparser.Error as err:
    #print(err)
    pass
  return config

def getLBLang():
  lang = "en"
  try:
    CONFIG_FILE = os.environ.get("LBSCONFIG")+"/general.cfg"
    config = readConfigFile(CONFIG_FILE)
    lang = config.get('BASE','LANG',fallback='de')
  except: pass
  return lang.upper()

def FOSHKpluginGetStatus(url):
  isStatus = ""
  try:
    r = requests.get(url)
    isStatus = r.text if r.status_code == 200 else ""
  except:
    pass
  return isStatus

def allPrint(s):
  s = hidePASSKEY(s)
  if loglog: logger.info(s)
  if rawlog: rawlogger.info(s)
  if sndlog: sndlogger.info(s)
  print(s)

def logPrint(s):
  s = hidePASSKEY(s)
  sub_in_s = False if LOG_IGNORE == [""] else bool([ele for ele in LOG_IGNORE if (ele in s)])
  if loglog and not sub_in_s:
    s_len = len(s)
    if (s_len >= 7 and LOG_LEVEL == "ERROR" and (s[:4] == "<OK>" or s[:7] == "<ERROR>" or s[:7] == "<DEBUG>")) or (s_len >= 9 and LOG_LEVEL == "WARNING" and (s[:4] == "<OK>" or s[:7] == "<ERROR>" or s[:7] == "<DEBUG>" or s[:9] == "<WARNING>")) or (s_len >= 9 and LOG_LEVEL == "INFO" and (s[:4] == "<OK>" or s[:7] == "<ERROR>" or s[:7] == "<DEBUG>" or s[:9] == "<WARNING>" or s[:6] == "<INFO>")) or LOG_LEVEL == "ALL": logger.info(s)
  if not sub_in_s or BUT_PRINT: print(s)                       # print always but don't log to file if filtered

def sndPrint(s, echo = False):
  s = hidePASSKEY(s)
  if sndlog:
    s_len = len(s)
    if (s_len >= 7 and LOG_LEVEL == "ERROR" and (s[:4] == "<OK>" or s[:7] == "<ERROR>" or s[:7] == "<DEBUG>")) or (s_len >= 9 and LOG_LEVEL == "WARNING" and (s[:4] == "<OK>" or s[:7] == "<ERROR>" or s[:7] == "<DEBUG>" or s[:9] == "<WARNING>")) or (s_len >= 9 and LOG_LEVEL == "INFO" and (s[:4] == "<OK>" or s[:7] == "<ERROR>" or s[:7] == "<DEBUG>" or s[:9] == "<WARNING>" or s[:6] == "<INFO>")) or LOG_LEVEL == "ALL": sndlogger.info(s)
  if echo: print(s)

def pushPrint(text):
  if PO_ENABLE:
    myLB_IP = socket.gethostbyname(socket.gethostname()) if LB_IP == "" else LB_IP
    myLink = "<a href=\"http://"+myLB_IP+":"+LBH_PORT+"/FOSHKplugin/help/\">"+myLB_IP+"</a> for ws@" if myLB_IP != "" and LBH_PORT != "" else ""
    text += "\n* "+myLink+WS_IP+"; "+time.strftime('%d.%m.%Y %H:%M:%S', time.localtime())+" *"
    t = threading.Thread(target=pushSend, args=(PO_URL, PO_TOKEN, PO_USER, text))
    t.start()

def pushSend(url, token, user, message):
  rcode = 0
  ret_str = ""
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      #r = requests.post(url, data = {"token": token,"user": user,"message": message}, timeout=httpTimeOut)
      r = requests.post(url, data = {"token": token,"user": user,"message": message,"html": "1"}, timeout=httpTimeOut)
      ret = str(r.status_code)
      ret_str = r.text
      if r.status_code in range(200,203): okstr = ""
      elif r.status_code in range(400,500): v = 400
    except requests.exceptions.Timeout as err:
      ret = "TIMEOUT"
    except requests.exceptions.ConnectionError as err:
      ret = "CONNERR"
    except requests.exceptions.RequestException as err:
      ret = "REQERR"
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  # only log if there were problems ...
  if ret_str == "": ret_str = "problem while sending push notification via Pushover"
  if okstr != "": logger.info(okstr + ret_str + " : " + ret + tries)
  return

def fr(s,l,c=" "):
  while len(s) < l: s+=c
  return s

def ftoc(f,n):                                           # convert Fahrenheit to Celsius
  out = "-9999"
  try:
    out = str(round((float(f)-32)*5/9.0,n))
  except ValueError: pass
  return out

def ctof(c,n):                                           # convert Celsius to Fahrenheit
  out = "-9999"
  try:
    out = str(round((float(c)*9/5.0) + 32,n))
  except ValueError: pass
  return out

def mphtokmh(f,n):                                       # convert mph to kmh
  return str(round(float(f)/0.621371,n))

def mphtoms(f,n):                                        # convert mph to m/s
  return str(round(float(f)/0.621371*1000/3600,n))

def intohpa(f,n):                                        # convert inHg to HPa
  return str(round(float(f)/0.02953,n))

def hpatoin(f,n):                                        # convert HPa to inHg 
  return str(round(float(f)/33.87,n))

def intomm(f,n):                                         # convert in to mm
  return str(round(float(f)/0.0393701,n))

def kmhtokts(f,n):
  out = "null"
  try:
    out = str(round((float(f))/1.852,n))
  except ValueError: pass
  return out

def kmhtomph(f,n):                                       # convert kmh to mph
  return str(round(float(f)/1.609,n))

def mmtoin(f,n):                                         # convert mm to in
  return str(round(float(f)/25.4,n))

def feettom(f,n):                                        # convert feet to m
  return str(round(float(f)/3.281,n))

def mtofeet(f,n):                                        # convert m to feet
  return str(round(float(f)*3.281,n))

def utcToLocal(utctime):
  offset = (-1*time.timezone)                            # Zeitzone ausgleichen
  if time.localtime(utctime)[8]: offset = offset + 3600  # Sommerzeit hinzu
  localtime = utctime + offset
  return localtime

def decHourToHMstr(sh):                                  # convert dec. hour to h:m
  f_sh = float(sh)
  sh_std = int(f_sh)
  sh_min = round((f_sh-int(f_sh))*60)
  return str(sh_std)+":"+str(sh_min)

def leafTo15(value):
  #try: out = str(round(int(value)/(6.6)))                # 99/15 = 6.6 as int
  try: out = str(round(float(value)/6.6,1))              # 99/15 = 6.6 as float
  except ValueError: out = ""
  return out

def loxTime(wert):
  # Gateway sendet UTC-Zeit; hier Umrechnung in Lokalzeit und dann nach Loxone
  try:
    wert=int(wert)
  except ValueError:
    return
  if wert > 31536000:                                          # groesser als 1 Jahr?
    offset = -time.timezone
    if time.localtime(wert)[8]:
      wert = wert + offset + 3600                              # 7200
    else:
      wert = wert + offset                                     # 3600
  return wert-1230768000 if LOX_TIME and wert >= 1230768000 else wert

def getSeparator(url, default = ""):
  if "separator=" in url:                                      # if found in url
    sep = url[url.index("separator=")+10:]
    if "&&" in sep:                                            # for & as separator
      i = sep.index("&&")
      sep = sep[:i+1]
    elif "&" in sep:                                           # ignore following fields
      i = sep.index("&")
      if i > 0: sep = sep[:i]
    sep = requests.utils.unquote(sep)
    if sep == "": sep = default
  elif default == "":                                          # take from config file
    sem = CSV_FIELDS.count(";")
    com = CSV_FIELDS.count(",")
    spa = CSV_FIELDS.count(" ")
    if sem >= com and sem >= spa: sep = ";"
    elif com >= sem and com >= spa: sep = ","
    else: sep = " "
  else:                                                        # if pre-defined
    sep = default
  if sep == "": sep = ";"                                      # fallback to ";"
  return sep

def getHeader(d,sep):
  s = ""
  for key,value in d.items(): s += key + ";"
  s = s[:-1]
  return s

def stringToDict(s,sep):
  # Parameter: s = String; sep = Separator (UDPstring = " "; WSstring = "&")
  # Output: d = dict
  d = dict(x.split("=") for x in s.split(sep)) if s != "" else {}
  return d

def getURLvalue(url,key):
  value = ""
  url_u = url.upper()
  key_u = key.upper()+"="
  if key_u in url_u:
    sub = url[url_u.index(key_u)+len(key_u):]
    value = sub[:sub.index("&")] if "&" in sub else sub
  return value

def killMyself():
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
  sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
  try:
    outstr = "restart initiated via UDP"
    if myDebug: logPrint("<DEBUG> "+outstr)
    sock.sendto(bytes("SID=FOSHKplugin,Plugin.shutdown", OutEncoding), (LB_IP, int(LBU_PORT)))
  except:
    outstr = "unable to restart via UDP"
    if myDebug: logPrint("<DEBUG> "+outstr)
    pass
  return outstr

def checkAmbientWeather(d):
  # q&d check if input is coming from Ambient Weather station
  return True if "AMBWeather" in str(d) else False

def checkBattery(d,FWver):
  # check known sensors if battery is still ok; if not fill outstring with comma-separated list of sensor names
  # Ambient macht ausschliesslich 0/1 wobei 1 = ok und 0 = low - mit Ausnahme von WH55 und WH57!
  # threshold for ws1900batt not confirmed
  if myDebug: logPrint("<DEBUG> checkBattery-FWver: " + FWver)
  isAmbientWeather = checkAmbientWeather(d)
  outstr = ""
  for key,value in d.items():
    if isAmbientWeather and ("batt" in key or "batleak" in key) and int(value) == 0 : outstr += key + " "
    # battery-reporting will be same for all weatherstations again after firmware-update of HP2551 v1.6.7
    #elif "EasyWeather" in FWver and "batt" in key and int(value) == 1 : outstr += key + " "
    else:
      if ("wh65batt" in key or "lowbatt" in key or "wh26batt" in key or "wh25batt" in key) and int(value) == 1 : outstr += key + " "
      elif "batt" in key and len(key) == 5 and int(value) == 1 and not isAmbientWeather: outstr += key + " "
      elif ("wh57batt" in key or "pm25batt" in key or "leakbatt" in key or "co2_batt" in key) and int(value) < 2: outstr += key + " "
      elif ("soilbatt" in key or "wh40batt" in key or "wh68batt" in key or "tf_batt" in key or "leaf_batt" in key) and float(value) <= 1.2: outstr += key + " "
      elif ("wh80batt" in key or "wh90batt" in key) and float(value) < 2.3: outstr += key + " "
      elif "ws1900batt" in key and float(value) < 2.5: outstr += key + " "
  return outstr.strip()

def trendSimple(d,start_pos,end_pos):
  groesser = kleiner = gleich = 0
  urwert = d[start_pos][1]                                     # der erste Wert im Zeitraum
  end_pos -= 1
  diff_wert = round(d[end_pos][1] - d[start_pos][1],1)         # Differenz zwischen akt. hPa und hist. hPa
  is3h = True if (end_pos - start_pos) * int(WS_INTERVAL) > 3600 else False
  # ohne Betrachtung der Einzelaenderungen
  if (is3h and diff_wert > 2) or (not is3h and diff_wert > 0.7): ret = 2
  elif (is3h and diff_wert > 0.7) or (not is3h and diff_wert > 0.2): ret = 1
  elif (is3h and diff_wert < -2) or (not is3h and diff_wert < -0.7): ret = -2
  elif (is3h and diff_wert < -0.7) or (not is3h and diff_wert < -0.2): ret = -1
  else: ret = 0
  return (ret,kleiner,gleich,groesser)

def trend(d,start_pos,end_pos):
  groesser = kleiner = 0
  gleich = 1
  urwert = d[start_pos][1]                                     # der erste Wert im Zeitraum
  end_pos -= 1
  diff_wert = round(d[end_pos][1] - d[start_pos][1],1)         # Differenz zwischen akt. hPa und hist. hPa
  is3h = True if (end_pos - start_pos) * int(WS_INTERVAL) > 3600 else False
  for i in range(start_pos,end_pos):
    vergleichswert = d[i][1]                                   # der jeweils aktuelle Wert
    if vergleichswert > urwert:
      groesser += 1                                            # count all values which > first entry
    elif vergleichswert < urwert:
      kleiner += 1                                             # count all values which < first entry
    else:
      gleich += 1                                              # count all values which = first entry
  if groesser > kleiner and groesser > gleich:                 # if most values are bigger than first entry then rising
    ret = 1
    if (is3h and diff_wert > 2) or (not is3h and diff_wert > 0.7): ret = 2
  elif kleiner > groesser and kleiner > gleich:                # if most values are smaller than first entry then falling
    ret = -1
    if (is3h and diff_wert < -2) or (not is3h and diff_wert < -0.7): ret = -2
  else:                                                        # if most values are equal to first entry then steady
    ret = 0
  #if myDebug:
  #  s3hstr = "3h" if is3h else "1h"
  #  logPrint("<DEBUG> trendN: (" + s3hstr + ") from: " + str(start_pos) + " to: " + str(end_pos) + ":" + " groesser: " + str(groesser) + " kleiner: " + str(kleiner) + " gleich: " + str(gleich) + " diff: " + str(diff_wert) + "hPa ret: " + str(ret))
  return (ret,kleiner,gleich,groesser)

def avgWind(d,w):                                              # get avg from deque d, field w
  s = 0
  l = len(d)
  for i in range(l):
    s = s + d[i][w]
  a = round(s/l,1)
  return a

def maxWind(d,w):                                              # get max value from deque d, field w
  s = 0
  l = len(d)
  for i in range(l):
    if d[i][w] > s: s = d[i][w]
  a = round(s,1)
  return a

def verStringToNum(s):                                         # extract version as comparable numeric value
  try:
    vpos = s.index("V")+1
    return(int(s[vpos:].replace(".","")))
  except ValueError:
    return

def getGW1100FWinfo(ipaddr, port, cur_ver):
  data = sendToWS(ipaddr,port,bytearray(cmd_get_MAC,'latin-1'))# gather MAC address
  mac = model = ver = ""
  try:
    for i in range(4,10):
      if data[i] < 10: mac += "0"
      mac += str(hex(data[i]))+":"                             # perhaps "%3A" instead of ":"
    mac = mac.replace("0x","").upper()[:-1]                    # then [:-3] of course
    model,ver = cur_ver.split("_")                             # strip model & version
  except: pass
  data = "id="+mac + "&model="+model + "&time="+str(int(time.time())) + "&user=1" + "&version="+ver
  # sign is not MD5 but a special encryption that FO does not want to publish
  sign = "&sign="+hashlib.md5(data.encode('utf-8')).hexdigest().upper()
  urlstr = "http://ota.ecowitt.net/api/ota/v1/version/info?" + data + sign
  #fw_info = requests.get(urlstr)
  class fw_info:                                               # spoof requests-response
    text = "unknown"
    url = urlstr
    status_code = 500
  return fw_info

def checkFWUpgrade():
  global updateWarning
  global rmt_ver
  cur_ver = rmt_ver = rmt_notes = ""
  # first check local data from weather station - stationtype in EW, softwaretype in WU = global var last_FWver
  if last_FWver != "": cur_ver = last_FWver                    # firmware is known from the data
  else:
    try:                                                       # firmware is unknown - so ask the weather station
      isFWver = sendToWS(WS_IP, WS_PORT, bytearray(cmd_get_FWver,'latin-1'))
      if myDebug: logPrint("<DEBUG> isFWver: " + str(isFWver) + " len: " + str(len(isFWver)))
      for i in range(5,5+isFWver[4]): cur_ver += chr(isFWver[i])
    except (ValueError, IndexError) as e:
      if myDebug: logPrint("<ERROR> problem in checkFWUpgrade: " + str(e))           # probably WS is not reachable or is not a GW1000
      cur_ver = ""
      pass
  if myDebug: logPrint("<DEBUG> current version found as *" + cur_ver + "*")
  if cur_ver != "":                                            # current version is known now
    if "GW1000" in cur_ver: model = "GW1000"
    elif "GW1100" in cur_ver: model = "GW1100"
    elif "GW2000" in cur_ver: model = "GW2000"
    elif "EasyWeather" in cur_ver: model = "EasyWeather"
    elif "AMBWeather" in cur_ver: model = "AMBWeather"
    elif "WH2650" in cur_ver: model = "WH2650"
    elif "WS1900" in cur_ver: model = "WS1900"
    elif "HP10" in cur_ver: model = "HP10"
    elif "WH2680" in cur_ver: model = "WH2680"
    elif "WH6006" in cur_ver: model = "WH6006"
    elif "WL6006" in cur_ver: model = "WL6006"
    else: model = "unknown"
    if myDebug: logPrint("<DEBUG> current model is identified as " + model)
    try:                                                       # now get firmware information from server
      fw_info = requests.get(UPD_URL)
      if myDebug: logPrint("<DEBUG> getting updinfo from " + UPD_URL + " results in " + str(fw_info.status_code))
      if fw_info.status_code == 200:
        config = configparser.ConfigParser(allow_no_value=True,strict=False)
        config.read_string(fw_info.text)
        rmt_ver = config.get(model,"VER",fallback="unknown")
        rmt_notes = config.get(model,"NOTES",fallback="").split(";")
        if rmt_ver == "unknown" and (model == "GW1100" or model == "GW2000"):         # if not offered this way, try the GW1100-way instead
          fw_info = getGW1100FWinfo(WS_IP, WS_PORT, cur_ver)
          try:
            d = json.loads(fw_info.text)
            rmt_ver = cur_ver.split("_")[0]+"_"+d.get("data").get("name")
          except: pass
          if myDebug:
            print("url:     "+fw_info.url)
            print("code:    "+str(fw_info.status_code))
            print("text:    *"+fw_info.text+"*")
            print("model:   "+model)
            print("current: "+cur_ver+" --> "+str(verStringToNum(cur_ver)))
            print("remote:  "+rmt_ver+" --> "+str(verStringToNum(rmt_ver)))
            print("notes:   "+str(rmt_notes))
        # for all found versions
        try:
          if rmt_ver != "unknown" and verStringToNum(rmt_ver) > verStringToNum(cur_ver):
            if model == "AMBWeather": use_app = "the app awnet"
            elif model == "GW1100" or model == "GW2000": use_app = "http://"+WS_IP+"/"
            else: use_app = "the app WS View or WSView Plus"
            logPrint("<WARNING> firmware update for " + model + " available - current: " + cur_ver + " avail: " + rmt_ver + " use " + use_app + " to update!")
            for i in range(len(rmt_notes)):
              logPrint("<WARNING> " + rmt_notes[i].strip())
            sendUDP("SID=" + defSID + " updatewarning=1 updateavail=" + rmt_ver + " time="  + str(loxTime(time.time())))
            push_str = "<WARNING> firmware update for " + model + " available - current: " + cur_ver + " avail: " + rmt_ver + " use " + use_app + " to update!\n"
            for i in range(len(rmt_notes)):
              push_str += rmt_notes[i].strip()+"\n"
            pushPrint(push_str)
            updateWarning = True
          else:
            sendUDP("SID=" + defSID + " updatewarning=0 time="  + str(loxTime(time.time())))
            updateWarning = False
            if myDebug: logPrint("<DEBUG> no newer update for " + model + " found - current: " + cur_ver + " avail: " + rmt_ver)
        except ValueError:
          if myDebug: logPrint("<DEBUG> except in inner try")
          pass
    except:
      if myDebug: logPrint("<DEBUG> except in outer try")
      pass
  return

# v0.07 execute script and exchange a string
def modExec(nr, script, outstr):
  # 2do: ggf. Parameter in script ermoeglichen
  try:
    if myDebug: logPrint("<DEBUG> FWD-"+nr+": script " + script + " started")
    cmd = subprocess.Popen([script, outstr], stdout=subprocess.PIPE, universal_newlines=True)
    newstr = cmd.communicate(timeout=execTimeOut)[0].splitlines()[-1]
    if myDebug: logPrint("<DEBUG> FWD-"+nr+": script " + script + " finished")
    if len(newstr) > 0 and outstr != newstr:
      outstr = newstr
      if newstr == execOnly and sndlog: sndPrint("FWD-"+nr+": " + "script " + script + " startet - data not forwarded : "+str(cmd.returncode))
      elif myDebug: logPrint("<DEBUG> FWD-"+nr+": script " + script + " altered the outgoing string")
    else:
      if myDebug: logPrint("<DEBUG> FWD-"+nr+": script " + script + " outgoing string unchanged")
  except OSError as e:
    sndPrint("<ERROR> FWD-"+nr+": Exec: " + e.strerror, True)    # something went wrong while calling script
    pass
  except subprocess.TimeoutExpired:
    sndPrint("<ERROR> FWD-"+nr+": Exec: script " + script + " not finished within " + str(execTimeOut) + " seconds", True)    # script ran into timeout
    pass
  return outstr

def checkLeakage(d):
  outstr = ""
  for i in range(1,5):
    i_s = str(i)
    value = getfromDict(d,["leak_ch"+i_s,"leak"+i_s])
    if value == "1":
      outstr += i_s+","
    if len(outstr) > 0 and outstr[-1] == ",": outstr = outstr[:-1]
  return outstr.strip()

def calcSunduration(sr, interval, currtime):                   ## Werner Krenn
  seuil = 0.0
  #SUN_COEF = 0.9
  erg = 0.0
  latitude = float(COORD_LAT) if COORD_LAT !="" else None
  longitude = float(COORD_LON) if COORD_LON !="" else None
  if sr is not None and latitude is not None and longitude is not None:
    utcdate = time.gmtime(currtime)
    dayofyear = time.localtime().tm_yday
    theta = 360 * dayofyear / 365
    equatemps = 0.0172 + 0.4281 * cos((pi / 180) * theta) - 7.3515 * sin(
               (pi / 180) * theta) - 3.3495 * cos(2 * (pi / 180) * theta) - 9.3619 * sin(2 * (pi / 180) * theta)
    corrtemps = longitude * 4
    declinaison = asin(0.006918 - 0.399912 * cos((pi / 180) * theta) + 0.070257 * sin(
               (pi / 180) * theta) - 0.006758 * cos(2 * (pi / 180) * theta) + 0.000908 * sin(
               2 * (pi / 180) * theta)) * (180 / pi)
    minutesjour = utcdate.tm_hour*60 + utcdate.tm_min
    tempsolaire = (minutesjour + corrtemps + equatemps) / 60
    angle_horaire = (tempsolaire - 12) * 15
    hauteur_soleil = asin(sin((pi / 180) * latitude) * sin((pi / 180) * declinaison) + cos(
               (pi / 180) * latitude) * cos((pi / 180) * declinaison) * cos((pi / 180) * angle_horaire)) * (180 / pi)
    #print("hateur: ", str(hauteur_soleil), " sr: ", str(sr), " sun_min: ", str(SUN_MIN))
    if float(hauteur_soleil) > 3 and float(sr) > float(SUN_MIN):
      try:
        seuil = (0.73 + 0.06 * cos((pi / 180) * 360 * dayofyear / 365)) *1080 * pow((sin(pi / 180) * hauteur_soleil), 1.25) * float(SUN_COEF)
      except:
        logPrint("<DEBUG> except in calcSunduration seuil")
        seuil = 0.0
      if float(sr) > seuil:                                    # groesser als Schwellwert - zaehlen
        erg = interval
        #logPrint("<DEBUG> Interval: " + str(interval) + " sr:"+ str(sr) + " seuil:" + str(seuil) + " SUN_COEF:" + str(SUN_COEF))
      else: erg = 0
    #if hauteur_soleil < 3: hauteur_soleil = 3
    #x1seuil = (0.73 + 0.06 * cos((pi / 180) * 360 * dayofyear / 365)) *1080
    #x2seuil = pow((sin(pi / 180) * hauteur_soleil), 1.25)
    #x3seuil = x1seuil * x2seuil * float(SUN_COEF) 
    #logPrint("<DEBUG> x1: "+ str(x1seuil) + " x2: " + str(x2seuil) + " x3: " + str(x3seuil))
  return erg

def convertDictToMetricDict(d_e,IGNORE_EMPTY=True,LOX_TIME=True):
  # 2do: bei eingehenden Ambient-Nachrichten stimmen die Keys in UDP-Ausgabe zu Loxone (vermutlich ueberall) nicht!
  global last_lightning_time
  global last_lightning
  global updateWarning
  ignoreValues=["-9999","None","null",""]
  d_m = {}
  for key,value in d_e.items():
    if IGNORE_EMPTY and value in ignoreValues:
      None
    elif key == "tempf": d_m.update({"tempc" : ftoc(value,1)})
    elif "temp1f" in key: d_m.update({"temp1c" : ftoc(value,1)})
    elif "temp2f" in key: d_m.update({"temp2c" : ftoc(value,1)})
    elif "temp3f" in key: d_m.update({"temp3c" : ftoc(value,1)})
    elif "temp4f" in key: d_m.update({"temp4c" : ftoc(value,1)})
    elif "temp5f" in key: d_m.update({"temp5c" : ftoc(value,1)})
    elif "temp6f" in key: d_m.update({"temp6c" : ftoc(value,1)})
    elif "temp7f" in key: d_m.update({"temp7c" : ftoc(value,1)})
    elif "temp8f" in key: d_m.update({"temp8c" : ftoc(value,1)})
    # ab v0.06 Vorbereitung auf WN34
    elif "tf_ch1" in key or "soiltemp1" in key: d_m.update({"tf_ch1c" : ftoc(value,1)})
    elif "tf_ch2" in key or "soiltemp2" in key: d_m.update({"tf_ch2c" : ftoc(value,1)})
    elif "tf_ch3" in key or "soiltemp3" in key: d_m.update({"tf_ch3c" : ftoc(value,1)})
    elif "tf_ch4" in key or "soiltemp4" in key: d_m.update({"tf_ch4c" : ftoc(value,1)})
    elif "tf_ch5" in key or "soiltemp5" in key: d_m.update({"tf_ch5c" : ftoc(value,1)})
    elif "tf_ch6" in key or "soiltemp6" in key: d_m.update({"tf_ch6c" : ftoc(value,1)})
    elif "tf_ch7" in key or "soiltemp7" in key: d_m.update({"tf_ch7c" : ftoc(value,1)})
    elif "tf_ch8" in key or "soiltemp8" in key: d_m.update({"tf_ch8c" : ftoc(value,1)})
    # v0.08 for WH6006
    elif "indoortempf" in key: d_m.update({"tempinc" : ftoc(value,1)})
    elif "tempinf" in key: d_m.update({"tempinc" : ftoc(value,1)})
    elif "windchillf" in key: d_m.update({"windchillc" : ftoc(value,1)})
    elif "feelslikef" in key: d_m.update({"feelslikec" : ftoc(value,1)})
    elif "dewptf" in key: d_m.update({"dewptc" : ftoc(value,1)})
    elif "heatindexf" in key: d_m.update({"heatindexc" : ftoc(value,1)})
    elif "baromin" in key: d_m.update({"baromhpa" : intohpa(value,2)})
    elif "baromrelin" in key: d_m.update({"baromrelhpa" : intohpa(value,2)})
    elif "barominrelin" in key: d_m.update({"baromrelhpa" : intohpa(value,2)})
    elif "baromabsin" in key: d_m.update({"baromabshpa" : intohpa(value,2)})
    elif "absbaro" in key: d_m.update({"baromabshpa" : intohpa(value,2)})
    elif "mph" in key: d_m.update({key.replace("mph","kmh") : mphtokmh(value,2)})
    elif "maxdailygust" in key: d_m.update({"maxdailygust" : mphtokmh(value,2)})
    elif "rainin" in key: d_m.update({key.replace("rainin","rainmm") : intomm(value,2)})
    elif "rainratein" in key: d_m.update({key.replace("rainratein","rainratemm") : intomm(value,2)})
    elif "dateutc" in key:
      # v0.07: Ambient WU-string contains "%20" - convert to " "
      # v0.08: WH6006 WU-string contains "%3A" - convert to ":"
      value = value.replace("%20","+").replace("%3A",":")
      isnow = time.strftime("%Y-%m-%d+%H:%M:%S",time.gmtime())
      d_m.update({key : isnow}) if value == "now" else d_m.update({key : value})
      if LOX_TIME:
        zeit = isnow if value == "now" else value
        d_m.update({"loxtime" : str(loxTime(utcToLocal(time.mktime(time.strptime(zeit, "%Y-%m-%d+%H:%M:%S")))))})
    elif "lightning_time" in key and value != "now" and value != "" and LOX_TIME:
      d_m.update({key : value})
      d_m.update({"lightning_loxtime" : str(loxTime(value))})
    # v0.07 new WH45 sensor tempf_co2 or with new key tf_co2
    #elif "tempf_co2" in key: d_m.update({"tempc_co2" : ftoc(value,1)})
    elif "tf_co2" in key: d_m.update({"tc_co2" : ftoc(value,1)})
    # Ambient-specific keys
    elif "soilhum" in key: d_m.update({key.replace("soilhum","soilmoisture") : value})
    elif key == "leak1": d_m.update({key.replace("leak1","leak_ch1") : value})
    elif key == "leak2": d_m.update({key.replace("leak2","leak_ch2") : value})
    elif key == "leak3": d_m.update({key.replace("leak3","leak_ch3") : value})
    elif key == "leak4": d_m.update({key.replace("leak4","leak_ch4") : value})
    elif key == "lightning_day": d_m.update({key.replace("lightning_day","lightning_num") : value})
    elif key == "lightning_distance": d_m.update({key.replace("lightning_distance","lightning") : value})
    elif "totalrain" in key: d_m.update({key.replace("totalrain","totalrainmm") : intomm(value,2)})
    elif key == "pm25": d_m.update({key.replace("pm25","pm25_ch1") : value})
    elif key == "pm25_24h": d_m.update({key.replace("pm25_24h","pm25_avg_24h_ch1") : value})
    elif key == "cloudf": d_m.update({"cloudm" : feettom(value,0)})
    elif key == "stationtype" or key == "softwaretype":
      if updateWarning and verStringToNum(value) == verStringToNum(rmt_ver):
        sendUDP("SID=" + defSID + " updatewarning=0 time="  + str(loxTime(time.time())))
        logPrint("<OK> firmware update to current version "+rmt_ver+" recognized - updatewarning state cleared")
        updateWarning = False
      # save the current firmware version as global var
      global last_FWver
      last_FWver = value
      d_m.update({key : value})
    # v0.09 WS90 compatibility - convert WS90 rain values to metric values
    elif "rain_piezo" in key: d_m.update({key.replace("rain_piezo","rain_piezomm") : intomm(value,2)})
    else:
      d_m.update({key : value})
  # to save some states in Config-file later
  global CONFIG_FILE
  config = readConfigFile(CONFIG_FILE)
  saved_lightning_time = config.get('Status','last_lightning_time',fallback="")
  try: saved_lightning_time = int(saved_lightning_time)
  except ValueError: saved_lightning_time = 0
  if not config.has_section("Status"): config.add_section('Status')
  haveToSave = False

  if SENSOR_WARNING:
    global inSensorWarning
    global preSensorWarning                                    # Vorwarnung um pm25batt zu beruhigen
    missingSensor = False                                      # wenn Sensorwerte der mandatory-Liste fehlen, warnen
    global SensorIsMissed                                      # fehlenden Sensor merken
    # 2do: ist ein missing Sensor wieder da aber zeitgleich ein anderer Sensor missed, erfolgt die "Wieder-da"-Meldung
    # fuer den neu als vermisst geltenden Sensor und nicht fuer den urspruenglich vermissten
    for i in range(len(senmand_arr)):
      if getfromDict(d_e,[senmand_arr[i]]) == "null":
        missingSensor = True
        SensorIsMissed = senmand_arr[i]
        #print("missing: " + senmand_arr[i])
        break
    if missingSensor:
      if not inSensorWarning and preSensorWarning:
        logPrint("<WARNING> missing data for mandatory sensor " + SensorIsMissed)
        sendUDP("SID=" + defSID + " sensorwarning=1 missed=" + SensorIsMissed + " time=" + str(loxTime(time.time())))
        pushPrint("<WARNING> missing data for mandatory sensor " + SensorIsMissed)
        inSensorWarning = True
        config.set("Status","inSensorWarning",str(inSensorWarning))
        config.set("Status","SensorIsMissed",SensorIsMissed)
        haveToSave = True
      elif not preSensorWarning:
        if myDebug: logPrint("<DEBUG> preWarning - mandatory sensor value " + SensorIsMissed + " missing; next time warn.")
        preSensorWarning = True
    elif inSensorWarning:
      logPrint("<OK> mandatory data for sensor " + SensorIsMissed + " is back again")
      sendUDP("SID=" + defSID + " sensorwarning=0 back=" + SensorIsMissed + " time=" + str(loxTime(time.time())))
      pushPrint("<OK> mandatory data for sensor " + SensorIsMissed + " is back again")
      config.remove_option("Status","inSensorWarning")
      config.remove_option("Status","SensorIsMissed")
      haveToSave = True
      inSensorWarning = False
      preSensorWarning = False
    else:
      preSensorWarning = False

  # new in v0.06 - battery-warning
  if BATTERY_WARNING:
    global inBatteryWarning
    SENSOR = checkBattery(d_m,last_FWver)
    if SENSOR != "":
      if not inBatteryWarning:
        logPrint("<WARNING> battery level for sensor(s) " + SENSOR + " is critical - please swap battery")
        sendUDP("SID=" + defSID + " batterywarning=1 critical=" + SENSOR + " time=" + str(loxTime(time.time())))
        pushPrint("<WARNING> battery level for sensor(s) " + SENSOR + " is critical - please swap battery")
        inBatteryWarning = True
        config.set("Status","inBatteryWarning",str(inBatteryWarning))
        haveToSave = True
    elif inBatteryWarning:
      logPrint("<OK> battery level for all sensors is ok again")
      sendUDP("SID=" + defSID + " batterywarning=0 time=" + str(loxTime(time.time())))
      pushPrint("<OK> battery level for all sensors is ok again")
      config.remove_option("Status","inBatteryWarning")
      haveToSave = True
      inBatteryWarning = False

  # new in v0.07 - leakage-warning
  if LEAKAGE_WARNING:
    global inLeakageWarning
    # von 1..4 durchgehen, wenn "1" dann Meldung generieren
    LEAKAGE = checkLeakage(d_m)
    if LEAKAGE != "":
      if not inLeakageWarning:
        logPrint("<WARNING> leakage reported for sensor(s) " + LEAKAGE + "!")
        sendUDP("SID=" + defSID + " leakagewarning=1 sensors=" + LEAKAGE + " time=" + str(loxTime(time.time())))
        pushPrint("<WARNING> leakage reported for sensor(s) " + LEAKAGE + "!")
        inLeakageWarning = True
        config.set("Status","inLeakageWarning",str(inLeakageWarning))
        haveToSave = True
    elif inLeakageWarning:
      logPrint("<OK> leakage remedied - leakage warning for all sensors cancelled")
      sendUDP("SID=" + defSID + " leakagewarning=0 time=" + str(loxTime(time.time())))
      pushPrint("<OK> leakage remedied - leakage warning for all sensors cancelled")
      config.remove_option("Status","inLeakageWarning")
      haveToSave = True
      inLeakageWarning = False

  # v0.08 co2 warning
  if CO2_WARNING:
    global inCO2Warning
    co2 = getfromDict(d_m,["co2","co2_in_aqin"])
    try:
      co2_num = float(co2)
      co2_lvl = float(CO2_WARNLEVEL)
    except ValueError:
      co2 = "null"
    if co2 != "null" and co2_num >= co2_lvl:
      if not inCO2Warning:
        logPrint("<WARNING> CO2 sensor reported a value higher than threshold: " + co2 + "/" + CO2_WARNLEVEL + "!")
        sendUDP("SID=" + defSID + " co2warning=1 co2current=" + co2 + " co2warnlevel=" + CO2_WARNLEVEL + " time=" + str(loxTime(time.time())))
        pushPrint("<WARNING> CO2 sensor reported a value higher than threshold: " + co2 + "/" + CO2_WARNLEVEL + " !")
        inCO2Warning = True
        config.set("Status","inCO2Warning",str(inCO2Warning))
        haveToSave = True
    elif inCO2Warning and co2 != "null" and co2_num <= co2_lvl-(co2_lvl/10):       # 10% hysteresis - value must be 10% below the limit value to cancel the warning
      logPrint("<OK> CO2 value is ok now (" + co2 + "/" + CO2_WARNLEVEL + ") - CO2 warning cancelled")
      sendUDP("SID=" + defSID + " co2warning=0 co2current=" + co2 + " co2warnlevel=" + CO2_WARNLEVEL + " time=" + str(loxTime(time.time())))
      pushPrint("<OK> CO2 value is ok now (" + co2 + "/" + CO2_WARNLEVEL + ") - CO2 warning cancelled")
      inCO2Warning = False
      config.remove_option("Status","inCO2Warning")
      haveToSave = True

  if STORM_WARNING:
    global stundenwerte
    global inStormWarning
    global inStorm3h
    global inStormTime
    global inStormWarnStart
    global last_hpaTrend1h
    global last_hpaTrend3h
    # add new item to list
    # befrieden durch Abschneiden der letzten Kommastelle
    # 2do: wenn baromrelhpa nicht vorhanden, darf auch CurDiff sowie Trend etc. nicht berechnet werden! - evtl. vorzeitig mit break raus?
    try:
      baromrelhpa = round(float(getfromDict(d_m,["baromrelhpa","baromhpa","pressure","baromrelin","baromin"])),1)
    except ValueError:
      baromrelhpa = -9999
    if baromrelhpa != -9999:
      stundenwerte.append([int(time.time()),baromrelhpa])        # save in UTC
      # v0.06: Trend aus allen verfuegbaren Werten ausgeben
      ago1h_avail = False
      ago3h_avail = False
      now_time = int(time.time())
      cur_pos = len(stundenwerte)                                # current position/index
      ago1h_pos = cur_pos - int(1*3600/int(WS_INTERVAL))         # position of data one hour before
      if ago1h_pos < 0:
        ago1h_pos = 0                                            # zu wenig Daten, daher das aelteste Datum nutzen
      else:
        ago1h_avail = True
      ago3h_pos = cur_pos - int(3*3600/int(WS_INTERVAL))         # position of data three hours before
      if ago3h_pos < 0:
        ago3h_pos = 0                                            # zu wenig Daten, daher das aelteste Datum nutzen
      else:
        ago3h_avail = True
      # Berechnungen fuer die letzte Stunde
      ago1h_time = stundenwerte[ago1h_pos][0]
      ago1h_baromrelhpa = stundenwerte[ago1h_pos][1]
      CurDiff1h = round(baromrelhpa - ago1h_baromrelhpa,1)
      time_diff1h = now_time - ago1h_time
      trend1h = trend(stundenwerte,ago1h_pos,cur_pos)
      hpaTrend1h = trend1h[0]
      t1h_kl = trend1h[1]
      t1h_gl = trend1h[2]
      t1h_gr = trend1h[3]
      # Berechnungen fuer die letzten 3 Stunden
      ago3h_time = stundenwerte[ago3h_pos][0]
      ago3h_baromrelhpa = stundenwerte[ago3h_pos][1]
      CurDiff3h = round(baromrelhpa - ago3h_baromrelhpa,1)
      time_diff3h = now_time - ago3h_time
      trend3h = trend(stundenwerte,ago3h_pos,cur_pos)
      hpaTrend3h = trend3h[0]
      t3h_kl = trend3h[1]
      t3h_gl = trend3h[2]
      t3h_gr = trend3h[3]
      # neue Felder fuer Ausgabe an Loxone etc. - only if EVAL_VALUES is active
      if EVAL_VALUES:
        d_m.update({"ptrend1" : str(hpaTrend1h)})
        d_m.update({"pchange1" : str(CurDiff1h)})
        wnow = WetterNow(baromrelhpa,myLanguage)
        d_m.update({'wnowlvl' : str(wnow[0])})
        d_m.update({'wnowtxt' : str(wnow[1])})
        if ago3h_avail:
          d_m.update({"ptrend3" : str(hpaTrend3h)})
          d_m.update({"pchange3" : str(CurDiff3h)})
          wprog = WetterPrognose(CurDiff3h,myLanguage)
          d_m.update({'wproglvl' : str(wprog[0])})
          d_m.update({'wprogtxt' : str(wprog[1])})
      # Auswertung
      if ago1h_avail and hpaTrend1h != last_hpaTrend1h:          # Werte fuer 1 Stunde vorhanden; Trendaenderung festgestellt
        #logPrint("<INFO> pressure 1h trend changed from " + str(last_hpaTrend1h) + " to " + str(hpaTrend1h))
        #sendUDP("SID=" + defSID + " ptrend1=" + str(hpaTrend1h) + " pchange1=" + str(CurDiff1h) + " time=" + str(loxTime(now_time)))
        last_hpaTrend1h = hpaTrend1h
      if ago3h_avail and hpaTrend3h != last_hpaTrend3h:          # Werte fuer 3 Stunden vorhanden; Trendaenderung festgestellt
        #logPrint("<INFO> pressure 3h trend changed from " + str(last_hpaTrend3h) + " to " + str(hpaTrend3h))
        #sendUDP("SID=" + defSID + " ptrend3=" + str(hpaTrend3h) + " pchange3=" + str(CurDiff3h) + " time=" + str(loxTime(now_time)))
        last_hpaTrend3h = hpaTrend3h
      if myDebug:
        doNothing()
        logPrint("<DEBUG> 1-old: " + str(ago1h_pos).rjust(3) + " " + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(ago1h_time)) + " " + str(ago1h_baromrelhpa) + "hPa now: " + str(cur_pos) + " " + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(now_time)) + " " + str(baromrelhpa) + "hPa diff1: " + str(time_diff1h).rjust(5) + "sec " + str(CurDiff1h) + "hPa" + " trend1: " + str(hpaTrend1h) + " <: " + str(t1h_kl) + " =: " + str(t1h_gl) + " >: " + str(t1h_gr))
        logPrint("<DEBUG> 3-old: " + str(ago3h_pos).rjust(3) + " " + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(ago3h_time)) + " " + str(ago3h_baromrelhpa) + "hPa now: " + str(cur_pos) + " " + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(now_time)) + " " + str(baromrelhpa) + "hPa diff3: " + str(time_diff3h).rjust(5) + "sec " + str(CurDiff3h) + "hPa" + " trend3: " + str(hpaTrend3h) + " <: " + str(t3h_kl) + " =: " + str(t3h_gl) + " >: " + str(t3h_gr))
        logPrint("<DEBUG> inStormWarning: " + str(inStormWarning) + " 3h: " + str(inStorm3h) + " now: " + str(int(time.time())) + " inStormTime: " + str(inStormTime) + " expire: " + str(STORM_EXPIRE*60))
      # 2do: Unterscheidung zw. 1h und 3h einbauen!
      if abs(CurDiff1h) > STORM_WARNDIFF or abs(CurDiff3h) > STORM_WARNDIFF3H:
        inStormTime = int(time.time())                           # should be UTC also
        if inStormWarnStart == 0: inStormWarnStart = inStormTime # save initial Warn-time
        # define reason for warning - instorm3h if CurDiff3h > STORM_WARNDIFF3H
        inStorm3h = True if abs(CurDiff3h) > STORM_WARNDIFF3H else False
        if myDebug:
          if inStorm3h:
            logPrint("<DEBUG> stormWarning: " + str(inStormWarning) + " agotime3 " + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(ago3h_time)) +": " + str(ago3h_baromrelhpa) + " --> " + str(baromrelhpa) + " diff: " + str(round(abs(CurDiff3h),3)) + "hPa" + " (> " + str(STORM_WARNDIFF) + ") inStormTime: " + str(inStormTime) + " StartWarning: " + str(inStormWarnStart))
          else:
            logPrint("<DEBUG> stormWarning: " + str(inStormWarning) + " agotime1 " + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(ago1h_time)) +": " + str(ago1h_baromrelhpa) + " --> " + str(baromrelhpa) + " diff: " + str(round(abs(CurDiff1h),3)) + "hPa" + " (> " + str(STORM_WARNDIFF) + ") inStormTime: " + str(inStormTime) + " StartWarning: " + str(inStormWarnStart))
        if not inStormWarning:
          if inStorm3h:
            what = "dropped" if CurDiff3h < 0 else "risen"
            logPrint("<WARNING> possible storm - air pressure has " + what + " more than " + str(STORM_WARNDIFF3H) + " hPa within three hours! (" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(ago3h_time)) +": " + str(ago3h_baromrelhpa) + " --> " + str(baromrelhpa) + " diff: " + str(CurDiff3h) + "hPa"")")
            sendUDP("SID=" + defSID + " stormwarning=1 time=" + str(loxTime(ago3h_time)))
            pushPrint("<WARNING> possible storm - air pressure has " + what + " more than " + str(STORM_WARNDIFF3H) + " hPa within three hours! (" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(ago3h_time)) +": " + str(ago3h_baromrelhpa) + " --> " + str(baromrelhpa) + " diff: " + str(CurDiff3h) + "hPa"")")
          else:
            what = "dropped" if CurDiff1h < 0 else "risen"
            logPrint("<WARNING> possible storm - air pressure has " + what + " more than " + str(STORM_WARNDIFF) + " hPa within one hour! (" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(ago1h_time)) +": " + str(ago1h_baromrelhpa) + " --> " + str(baromrelhpa) + " diff: " + str(CurDiff1h) + "hPa"")")
            sendUDP("SID=" + defSID + " stormwarning=1 time=" + str(loxTime(ago1h_time)))
            pushPrint("<WARNING> possible storm - air pressure has " + what + " more than " + str(STORM_WARNDIFF) + " hPa within one hour! (" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(ago1h_time)) +": " + str(ago1h_baromrelhpa) + " --> " + str(baromrelhpa) + " diff: " + str(CurDiff1h) + "hPa"")")
          inStormWarning = True
          config.set("Status","inStormWarning",str(inStormWarning))
          config.set("Status","inStorm3h",str(inStorm3h))
          config.set("Status","inStormWarnStart",str(inStormWarnStart))
          config.set("Status","inStormTime",str(inStormTime))
          haveToSave = True
      elif inStormWarning and int(time.time()) >= inStormTime + STORM_EXPIRE*60:
        now = int(time.time())
        inStormDuration = int((now-inStormWarnStart)/60)         # now better?
        if inStorm3h:
          logPrint("<OK> storm warning cancelled after " + str(inStormDuration) + " minutes (" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(ago3h_time)) +": " + str(ago3h_baromrelhpa) + " --> " + str(baromrelhpa) + " diff: " + str(CurDiff3h) + "hPa"")")
          sendUDP("SID=" + defSID + " stormwarning=0 time=" + str(loxTime(now)) + " start=" + str(loxTime(inStormWarnStart)) + " end=" + str(loxTime(now)) + " last=" + str(loxTime(ago3h_time)))
          pushPrint("<OK> storm warning cancelled after " + str(inStormDuration) + " minutes (" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(ago3h_time)) +": " + str(ago3h_baromrelhpa) + " --> " + str(baromrelhpa) + " diff: " + str(CurDiff3h) + "hPa"")")
        else:
          logPrint("<OK> storm warning cancelled after " + str(inStormDuration) + " minutes (" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(ago1h_time)) +": " + str(ago1h_baromrelhpa) + " --> " + str(baromrelhpa) + " diff: " + str(CurDiff1h) + "hPa"")")
          sendUDP("SID=" + defSID + " stormwarning=0 time=" + str(loxTime(now)) + " start=" + str(loxTime(inStormWarnStart)) + " end=" + str(loxTime(now)) + " last=" + str(loxTime(ago1h_time)))
          pushPrint("<OK> storm warning cancelled after " + str(inStormDuration) + " minutes (" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(ago1h_time)) +": " + str(ago1h_baromrelhpa) + " --> " + str(baromrelhpa) + " diff: " + str(CurDiff1h) + "hPa"")")
        inStormWarnStart = 0
        config.remove_option("Status","inStormWarning")
        config.remove_option("Status","inStorm3h")
        config.remove_option("Status","inStormWarnStart")
        config.remove_option("Status","inStormTime")
        haveToSave = True
        inStormWarning = False
        inStorm3h = False

  if TSTORM_WARNING:
    global inTSWarning
    global inTSWarnStart
    global inTS_lightning_num
    global ldmin
    global ldmax
    global ldsum
    global ldavg
    # get current values
    try:
      lightning_num = int(getfromDict(d_m,["lightning_num","lightning_day"]))        # lightning-count per day (automatically reset at 00:00)
    except ValueError:
      lightning_num = 0
    try:
      lightning = int(getfromDict(d_m,["lightning","lightning_distance"]))           # distance in km of last lightning-event
    except ValueError:
      lightning = 0
    try:
      lightning_time = int(getfromDict(d_m,["lightning_time"]))                      # time of last lightning-event - could be empty!
    except ValueError:
      lightning_time = 0
    if inTSWarning and lightning_num < inTS_lightning_num:                           # overnight thunderstorm - lightning_num was reset to 0 at midnight
      if myDebug: logPrint("<DEBUG> lightning_num (" + str(lightning_num) + ") < inTS_lightning_num (" + str(inTS_lightning_num) + ") - overnight thunderstorm")
      inTS_lightning_num += lightning_num
    elif inTSWarning:                                                                # while in warning state
      inTS_lightning_num = lightning_num
    else:                                                                            # not in warning state, so do not count lightnings
      inTS_lightning_num = 0
    # compare old time with current time
    now = int(time.time())
    if lightning_time > last_lightning_time:                   # there was a lightning
      if not inTSWarning:                                      # not yet in warning state
        # activate warning when the requirements are met (count >= warncount & distance >= warndist
        if lightning_num >= TSTORM_WARNCOUNT and lightning <= TSTORM_WARNDIST:
          # there is a thunderstorm-condition
          ldmin = lightning
          ldmax = lightning
          ldsum = lightning
          inTSWarnStart = int(time.time())
          logPrint("<WARNING> thunderstorm recognized (start=" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(inTSWarnStart)) +")")
          sendUDP("SID=" + defSID + " tswarning=1 time=" + str(loxTime(inTSWarnStart)))
          pushPrint("<WARNING> thunderstorm recognized (start=" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(inTSWarnStart)) +")")
          inTSWarning = True
          config.set("Status","inTSWarning",str(inTSWarning))
          config.set("Status","inTSWarnStart",str(inTSWarnStart))
          config.set("Status","inTS_lightning_num",str(inTS_lightning_num))
          config.set("Status","last_lightning_time",str(last_lightning_time))
          config.set("Status","last_lightning",str(last_lightning))
          haveToSave = True
      else:                                                    # another lightning in warning state
        # just count lightnings
        if lightning < ldmin: ldmin = lightning                # note the minimum distance
        if lightning > ldmax: ldmax = lightning                # note the maximum distance
        ldsum = ldsum + lightning                              # sum up all distances
      last_lightning_time = lightning_time
      last_lightning = lightning
    elif inTSWarning and now >= last_lightning_time + TSTORM_EXPIRE*60:              # there was no lightning and expire time is over
      inTSWarnDuration = int((now-inTSWarnStart)/60)    # now better?
      logPrint("<OK> thunderstorm warning cancelled after " + str(inTSWarnDuration) + " minutes (start=" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(inTSWarnStart)) + " end=" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(now)) + " last=" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(last_lightning_time)) + " lcount=" + str(inTS_lightning_num) + " ldmin=" + str(ldmin) + " ldmax=" + str(ldmax) + ")")
#" ldavg=" + str(round(ldavg,1)) + ")")
      sendUDP("SID=" + defSID + " tswarning=0 time=" + str(loxTime(now)) + " start=" + str(loxTime(inTSWarnStart)) + " end=" + str(loxTime(now)) + " last=" + str(loxTime(last_lightning_time)) + " lcount=" + str(inTS_lightning_num) + " ldmin=" + str(ldmin) + " ldmax=" + str(ldmax))
# + " ldavg=" + str(round(ldavg,1)))
      pushPrint("<OK> thunderstorm warning cancelled after " + str(inTSWarnDuration) + " minutes (start=" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(inTSWarnStart)) + " end=" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(now)) + " last=" + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(last_lightning_time)) + " lcount=" + str(inTS_lightning_num) + " ldmin=" + str(ldmin) + " ldmax=" + str(ldmax) + ")")
#" ldavg=" + str(round(ldavg,1)) + ")")
      config.remove_option("Status","inTSWarning")
      config.remove_option("Status","inTSWarnStart")
      config.remove_option("Status","inTS_lightning_num")
      haveToSave = True
      inTSWarnStart = 0
      inTS_lightning_num = 0                                   # reset the lightning count to 0 again
      inTSWarning = False
    # average of lightning_distance
    if inTS_lightning_num > 0:
      ldavg = float(ldsum / inTS_lightning_num)
    else:
      ldavg = 0
    if myDebug: logPrint("<DEBUG> inTSWarning: " + str(inTSWarning) + " cnt: " + str(lightning_num) + " dist: " + str(lightning) + " time: " + time.strftime("%d.%m.%Y %H:%M:%S",time.localtime(lightning_time)) + " lcount=" + str(inTS_lightning_num) + " ldmin=" + str(ldmin) + " ldmax=" + str(ldmax) + " ldsum=" + str(ldsum) + " ldavg=" + str(round(ldavg,1)))

    # Gewittervorhersage; wenn Taupunkt 21,1°C (70F) überschreitet
    # only if WH57 is not present:
    #if getfromDict(d_e,["wh57batt"]) == "null":
    #try:
    #  dp = float(getfromDict(d_m,["dewptc"]))
    #  #if myDebug: logPrint("<DEBUG> dewpoint is currently: " + str(dp) + "°C")
    #  if dp > 21.1: logPrint("<WARNING> possible thunderstorm - dewpoint > 21.1°C (" + str(dp) + ")")
    #except ValueError:
    #  pass

  # v0.07 - save last known lightning data
  if FIX_LIGHTNING:
    # save last known lightning values
    try:
      lightning_time = int(getfromDict(d_m,["lightning_time"]))                # time of last lightning-event - could be empty!
      lightning = int(getfromDict(d_m,["lightning","lightning_distance"]))     # distance in km of last lightning-event
      if lightning_time > saved_lightning_time:                                # save status in Config-file if new lightning detected
        config.set("Status","last_lightning_time",str(lightning_time))
        config.set("Status","last_lightning",str(lightning))
        haveToSave = True
        if myDebug: logPrint("<DEBUG> saved lightning data " + str(lightning_time) + "/" + str(lightning) + " to config-file")
    except ValueError:
      pass

  # save status in Config-file
  if haveToSave:
    with open(CONFIG_FILE, "w") as configfile: config.write(configfile)
  return d_m

def forwardDictToMeteoTemplate(url,d_in,fwd_sid,fwd_pwd,script,nr,ignoreKeys,remapKeys):
  # convert incoming metric dict to MeteoTemplate
  d = remappedDict(d_in,remapKeys,nr)                          # remap keys in current dictionary
  if not "PASS=" in url: url += "?PASS="+str(fwd_pwd) + "&"
  outstr = ""
  isAmbientWeather = checkAmbientWeather(d)
  dontuse = ("PASSKEY","PASSWORD","ID","model","freq")
  ignoreValues=["-9999","None","null"]
  for key,value in d.items():
    if key in ignoreKeys or key in dontuse or (IGNORE_EMPTY and value in ignoreValues):
      None
    elif key == "PASS":
      # possibility to exchange PASS?
      None
    elif key == "dateutc":
      # convert time string to unixdate (UTC)
      if value == "now":
        isnow = time.strftime("%Y-%m-%d+%H:%M:%S",time.gmtime())
        istime = utcToLocal(time.mktime(time.strptime(isnow, "%Y-%m-%d+%H:%M:%S")))
      else:
        istime = utcToLocal(time.mktime(time.strptime(value.replace("%20","+").replace("%3A",":"), "%Y-%m-%d+%H:%M:%S")))
      istime = int(istime)
      outstr += "U=" + str(istime) + "&"
    elif key == "tempc":
      outstr += "T=" + str(value) + "&"
    elif key == "humidity":
      outstr += "H=" + str(value) + "&"
    elif key == "baromhpa" or key == "baromrelhpa":
      outstr += "P=" + str(value) + "&"
    elif key == "baromabsin" or key == "baromabshpa":
      outstr += "UGP=" + str(value) + "&"
    elif key == "windspeedkmh":
      outstr += "W=" + str(value) + "&"
    elif key == "windgustkmh":
      outstr += "G=" + str(value) + "&"
    elif key == "winddir":
      outstr += "B=" + str(value) + "&"
    elif key == "dailyrainmm":
      outstr += "R=" + str(value) + "&"
    elif key == "rainratemm":
      outstr += "RR=" + str(value) + "&"
    elif key == "solarradiation" or key == "solarRadiation":
      outstr += "S=" + str(value) + "&"
    elif key == "UV" or key == "uv":
      outstr += "UV=" + str(value) + "&"
    elif key == "tempinc":
      outstr += "TIN=" + str(value) + "&"
    elif key == "humidityin" or key == "indoorhumidity":
      outstr += "HIN=" + str(value) + "&"
    elif "temp" in key and len(key) == 6 and key[-1] == "c":
      outstr += "T" + str(key[4]) + "=" + str(value) + "&"
    elif "humidity" in key and len(key) == 9:
      outstr += "H" + str(key[8]) + "=" + str(value) + "&"
    elif "tf_ch" in key and len(key) == 6:
      outstr += "TS" + str(key[6]) + "=" + str(value) + "&"
    elif "soilmoisture" in key and len(key) == 13:
      outstr += "SM" + str(key[12]) + "=" + str(value) + "&"
    elif key == "lightning_day" or key == "lightning_num":
      outstr += "L=" + str(value) + "&"
    elif "pm25_ch" in key and len(key) == 8:
      outstr += "PP" + str(key[7]) + "=" + str(value) + "&"
    elif key == "pm25":
      outstr += "PP1=" + str(value) + "&"
    elif key == "co2":
      outstr += "CO2_1=" + str(value) + "&"
    elif "leafwetness_ch" in key and len(key) == 15:
      outstr += "LW" + str(key[14]) + "=" + str(leafTo15(value)) + "&"
    elif key == "sunhours":
      outstr += "SS=" + str(value) + "&"
    elif key == "lightning" or key == "lightning_dist":
      outstr += "LD=" + str(value) + "&"
    elif key == "lightning_time":
      outstr += "LT=" + str(value) + "&"
    # battery data - send OK or LOW
    elif key == "wh65batt" or key == "battout" or key == "wh26batt" or key == "wh25batt":
      battval = "OK" if value == "0" else "LOW"
      outstr += "TBAT=" + battval + "&"
    elif key == "wh68batt":
      battval = "OK" if float(value) > 1.2 else "LOW"
      outstr += "WBAT=" + battval + "&"
    elif key == "wh80batt":
      battval = "OK" if float(value) > 2.2 else "LOW"
      outstr += "WBAT=" + battval + "&"
    elif key == "wh90batt":
      battval = "OK" if float(value) > 2.2 else "LOW"
      outstr += "WBAT=" + battval + "&"
    elif key == "wh40batt":
      battval = "OK" if float(value) > 1.2 else "LOW"
      outstr += "RBAT=" + battval + "&"
    elif key == "wh57batt":
      battval = "OK" if int(value) >= 2 else "LOW"
      outstr += "LBAT=" + battval + "&"
    elif key == "batt_lightning":
      battval = "OK" if value == "0" else "LOW"
      outstr += "LBAT=" + battval + "&"
    elif "soilbatt" in key and len(key) == 9:
      battval = "OK" if float(value) > 1.2 else "LOW"
      outstr += "SM" + str(key[8]) + "BAT=" + battval + "&"
    elif "pm25batt" in key and len(key) == 9:
      battval = "OK" if int(value) >= 2 else "LOW"
      outstr += "PP" + str(key[8]) + "BAT=" + battval + "&"
    # 2do: Ambient Weather sends 1 for ok and 0 for low
    elif "battsm" in key and len(key) == 7:
      battval = "OK" if value == "1" else "LOW"                # should be ok
      outstr += "SM" + str(key[6]) + "BAT=" + battval + "&"
    elif key == "batt_25":
      battval = "OK" if value == "1" else "LOW"                # should be ok
      outstr += "PM1BAT=" + battval + "&"
    elif "batt" in key and len(key) == 5:                      # Ecowitt sends 0 for OK but Ambient sends 1 for OK
      if isAmbientWeather:
        battval = "OK" if value == "1" else "LOW"
      else:
        battval = "OK" if value == "0" else "LOW"
      outstr += "T" + str(key[4]) + "BAT=" + battval + "&"
    elif "tf_batt" in key and len(key) == 8:
      battval = "OK" if value == "0" else "LOW"
      outstr += "TS" + str(key[7]) + "BAT=" + battval + "&"
    elif "leaf_batt" in key and len(key) == 10:
      battval = "OK" if value == "0" else "LOW"
      outstr += "LW" + str(key[9]) + "BAT=" + battval + "&"
    elif key == "softwaretype":
      outstr += "SW=" + str(value) + "&"
    else:
      if myDebug: logPrint("<DEBUG> forwardDictToMeteoTemplate: unknown field: " + str(key) + " with value: " + str(value))
  if len(outstr) > 0 and outstr[-1] == "&": outstr = outstr[:-1]
  # add programname and version as SW (like weewx does)
  if "&SW=" not in outstr: outstr += "&SW="+prgname+"-"+prgver
  if script != "":
    outstr = modExec(nr, script, outstr)                       # modify outstr with external script before sending
    if outstr == execOnly: return                              # just run the exec-script but do not forward the string
  ret = ""
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      r = requests.get(url+outstr,timeout=httpTimeOut)
      ret = str(r.status_code)
      okstr = "<ERROR> " if r.status_code not in range(200,203) else ""
      if r.status_code in range(400,500): v = 400
    except requests.exceptions.Timeout as err:
      ret = "TIMEOUT"
    except requests.exceptions.ConnectionError as err:
      ret = "CONNERR"
    except requests.exceptions.RequestException as err:
      ret = "REQERR"
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  if sndlog: sndPrint(okstr + "FWD-"+nr+": " + url + outstr + " : " + ret + tries)
  return

def forwardDictToWC(url,d_in,fwd_sid,fwd_pwd,script,nr,ignoreKeys,remapKeys):
  # convert incoming metric dict to WeatherCloud
  # wid, key, tempin, humin, bar, temp, hum, dew, chill, heat, solarrad, uvi, wspd, wspdavg, windgustmph, wspdhi, wdir, wdiravg, rainrate, rain, date, time, type, ver
  d = remappedDict(d_in,remapKeys,nr)                          # remap keys in current dictionary
  if not "wid=" in url and not "key=" in url: url += "?wid="+str(fwd_sid)+"&key="+str(fwd_pwd) + "&"
  outstr = ""
  tempinc = -9999
  humidityin = -9999
  dontuse = ("PASSKEY","PASSWORD","ID","model","freq")
  ignoreValues=["-9999","None","null"]
  for key,value in d.items():
    if key in ignoreKeys or key in dontuse or (IGNORE_EMPTY and value in ignoreValues):
      None
    elif key == "wid":
      # possibility to exchange wid?
      None
    elif key == "dateutc":
      # convert time string to date-string and separate time-string
      if value == "now":
        isnow = time.gmtime()
        isdate = time.strftime('%Y%m%d', isnow)
        istime = time.strftime('%H%M', isnow)
      else:
        value = value.replace("%20","+").replace("%3A",":")
        isdate = value[0:4] + value[5:7] + value[8:10]
        istime = value[11:13] + value[14:16]                   # + value[17:19]
      if (len(value) == 19 and value[4] == "-" and value[7] == "-" and value[13] == ":" and value[16] == ":") or value == "now":
        outstr += "date=" + str(isdate) + "&" + "time=" + str(istime) + "&"
    elif key == "tempc":
      val = round(float(value)*10)
      outstr += "temp=" + str(val) + "&"
    elif key == "dewptc":
      val = round(float(value)*10)
      outstr += "dew=" + str(val) + "&"
    elif key == "windchillc":
      val = round(float(value)*10)
      outstr += "chill=" + str(val) + "&"
    elif key == "feelslikec":                                  # this is (hopefully) what they call heat
      val = round(float(value)*10)
      outstr += "heat=" + str(val) + "&"
    elif key == "humidity":
      outstr += "hum=" + str(value) + "&"
    elif key == "baromhpa" or key == "baromrelhpa":
      val = round(float(value)*10)
      outstr += "bar=" + str(val) + "&"
    elif key == "windspeedkmh":
      outstr += "wspd=" + str(round(float(value)/3.6*10)) + "&"
    elif key == "windspdkmh_avg10m":
      outstr += "wspdavg=" + str(round(float(value)/3.6*10)) + "&"
    elif key == "windgustkmh_max10m":
      outstr += "wspdhi=" + str(round(float(value)/3.6*10)) + "&"
    elif key == "winddir":
      outstr += "wdir=" + str(value) + "&"
    elif key == "winddir_avg10m":
      outstr += "wdiravg=" + str(value) + "&"
    elif key == "dailyrainmm":
      outstr += "rain=" + str(round(float(value)*10)) + "&"
    elif key == "rainratemm":
      outstr += "rainrate=" + str(round(float(value)*10)) + "&"
    elif key == "solarradiation" or key == "solarRadiation":
      val = round(float(value)*10)
      outstr += "solarrad=" + str(val) + "&"
    elif key == "UV" or key == "uv":
      val = round(float(value)*10)
      outstr += "uvi=" + str(val) + "&"
    elif key == "tempinc":
      tempinc = value
      val = round(float(value)*10)
      outstr += "tempin=" + str(val) + "&"
    elif key == "humidityin" or key == "indoorhumidity":
      humidityin = value
      outstr += "humin=" + str(value) + "&"
    elif "temp" in key and len(key) == 6 and key[-1] == "c":                  
      val = round(float(value)*10)
      outstr += "temp" + str(key[4]) + "=" + str(val) + "&"
      # according API-doc v0.7 this should be:
      #outstr += "temp" + "0" + str(int(key[4])+1) + "=" + str(val) + "&"
    elif "humidity" in key and len(key) == 9:
      outstr += "hum" + str(key[8]) + "=" + str(value) + "&"
      # according API-doc v0.7 this should be:
      #outstr += "hum" + "0" + str(int(key[8])+1) + "=" + str(value) + "&"
    elif "soilmoisture" in key and len(key) == 13:
      if key[12] == "1":
        outstr += "soilmoist" + "=" + str(value) + "&"
      else:
        outstr += "soilmoist" + "0" + str(key[12]) + "=" + str(value) + "&"
    # v0.07: for Ambient
    elif "soilhum" in key and len(key) == 8:
      if key[7] == "1":
        outstr += "soilmoist" + "=" + str(value) + "&"
      else:
        outstr += "soilmoist" + "0" + str(key[7]) + "=" + str(value) + "&"
    # v0.07: WN35-compatibility
    elif "leafwetness_ch" in key and len(key) == 15:
      if key[14] == "1":
        outstr += "leafwet" + "=" + str(leafTo15(value)) + "&"
      else:
        outstr += "leafwet" + "0" + str(key[14]) + "=" + str(leafTo15(value)) + "&"
    # v0.08: WH45 air quality sensor
    elif "pm25_co2" in key or "pm25_in_aqin" in key:
      outstr += "pm25" + "=" + str(value) + "&"
    elif "pm10_co2" in key or "pm10_in_aqin" in key:
      outstr += "pm10" + "=" + str(value) + "&"
    elif "pm25_AQI_co2" in key:
      outstr += "aqi" + "=" + str(value) + "&"
    elif key == "co2" or key == "co2_in_aqin":
      outstr += "co2" + "=" + str(value) + "&"
    else:
      if myDebug: logPrint("<DEBUG> forwardDictToWC: unknown field: " + str(key) + " with value: " + str(value))
  if len(outstr) > 0 and outstr[-1] == "&": outstr = outstr[:-1]
  # calculate dew & heatindex for inside
  if EVAL_VALUES and tempinc != -9999 and humidityin != -9999:
    try:
      dewin = int(float(ftoc(getDewPointF(float(ctof(tempinc,1)), float(humidityin)),1))*10)
      heatin = int(float(ftoc(getHeatIndex(float(ctof(tempinc,1)), float(humidityin)),1))*10)
      outstr += "&dewin=" + str(dewin) + "&heatin=" + str(heatin)
    except ValueError: pass
  #if not ("&type=" in outstr and "&ver=" in outstr):
  outstr += "&type=" + prgname + "&ver=" + prgver
  if script != "":
    outstr = modExec(nr, script, outstr)                       # modify outstr with external script before sending
    if outstr == execOnly: return                              # just run the exec-script but do not forward the string
  ret = ""
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      headers = {'Connection': 'Close','User-Agent': None}
      # strange problems if header contains Connection:Close - so disable for test
      #r = requests.get(url+outstr,headers=headers,timeout=httpTimeOut)
      r = requests.get(url+outstr,timeout=httpTimeOut)
      # WC responds status_code 200 in any case - real return code is in text
      ret = r.text.strip()
      if ret != "200":
        okstr = "<ERROR> "
        v = 400
      else: okstr = ""
    except requests.exceptions.Timeout as err:
      ret = "TIMEOUT"
    except requests.exceptions.ConnectionError as err:
      ret = "CONNERR"
    except requests.exceptions.RequestException as err:
      ret = "REQERR"
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  if sndlog: sndPrint(okstr + "FWD-"+nr+": " + url + outstr + " : " + ret + tries)
  return

def dictToString(d,sep,klammern=False,ignoreKeys={},ignoreValues={},withkey=True,withvalue=True,hideSpace=False):
  s = ""
  sep_len = len(sep)
  try:
    for key,value in d.items():
      if key not in ignoreKeys:
        s_value = str(value)
        if s_value not in ignoreValues:
          if withkey and withvalue:
            if klammern and " " in s_value:
              s += key + "=" + "\"" + s_value + "\"" + sep
            else:
              if hideSpace and " " in s_value:
                s += key + "=" + s_value.replace(" ","%20") + sep
              else:
                s += key + "=" + s_value + sep
          elif withkey:
            s += key + sep
          elif klammern and " " in s_value:
            s += "\"" + s_value + "\"" + sep
          else:
            s += s_value + sep
    # remove last sep
    if len(s) >= sep_len and s[-sep_len:] == sep: s = s[:-sep_len]
  except: pass
  return s

def lineToCSV(d, felder):
  # Parameter: d = Dictionary; felder = zu exportierende Felder
  # Output: s = CSV-String
  s = ""
  if ";" in felder:
    sep = ";"
  elif "," in felder:
    sep = ","
  elif " " in felder:
    sep = " "
  else:
    sep = ""
  if sep != "":
    a = felder.split(sep)
    for i in range(len(a)):
      if a[i] in d:
        wert = str(d[a[i]])
        # don't replace "." with "," for theses fields
        if sep == ";" and a[i] != "stationtype" and a[i] != "softwaretype" and a[i] != "model":
          wert = wert.replace(".",",")
        s += wert
        if i < len(a)-1: s += sep
      else:
        if i < len(a)-1: s += ""+sep
    s = time.strftime("%d.%m.%Y %H:%M:%S") + sep + s
  return s

def checkLBP_PATH(pname,pdir):
  s = ""
  try:
    db = json.load(open(os.environ.get("LBSDATA")+"/plugindatabase.json"))
    for plugin in db['plugins']:
      if db['plugins'][plugin]['name'] == pname:
        s = db['plugins'][plugin]['directories'][pdir]
        if s != "": s += "/"
        break
  except:
    try:
      dbfile = open(os.environ.get("LBSDATA")+"/plugindatabase.dat", "r")
      for line in dbfile:
        a = line.split("|")
        if len(a) >= 5 and a[4] == pname:
          env = "LBPTEMPLA" if pdir == "lbptemplatedir" else pdir.replace("dir","").upper()
          s = os.environ.get(env)
          if s == None: s = ""
          if s != "": s += "/" + a[5] + "/"
          break
      dbfile.close()
    except: pass
  return s

def replaceSpace(s):                                           # replace all " " with "%20" and """ with "%22"
  if "\"" in s:
    isin = True
    first_pos = 0
    while "\"" in s:
      first_pos = s.index("\"",first_pos)
      last_pos = s.index("\"",first_pos+1)+1
      vorher = s[first_pos:last_pos]
      nachher = vorher.replace(" ","%20").replace("\"","%22")
      s = s.replace(vorher,nachher)
  else:
    isin = False
  return (isin,s)

def sendUDP(UDPstr):
  # Leerzeichen innerhalb von doppelten Anfuehrungszeichen muessen mit %20 ersetzt werden, damit nicht innerhalb eines values getrennt wird
  if UDP_ENABLE:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    isin,s = replaceSpace(UDPstr)
    s_len = len(s)
    sid_end = s.index(" ")
    sid = s[:sid_end]
    s = s[sid_end:]
    while len(s) > UDP_MAXLEN:
      s_pos = UDP_MAXLEN
      while s_pos < len(s) and s[s_pos] != " ":
        s_pos += 1
      s_sub = s[:s_pos]
      if isin:
        s_sub = s_sub.replace("%20"," ")
        s_sub = s_sub.replace("%22","\"")
      try:
        #sock.sendto(bytes(sid+s[:s_pos], OutEncoding), (LOX_IP, int(LOX_PORT)))
        sock.sendto(bytes(sid+s_sub, OutEncoding), (LOX_IP, int(LOX_PORT)))
      except:
        logPrint("<ERROR> sendUDP to "+LOX_IP+":"+LOX_PORT+" im except!")
      s = s[s_pos:]
    if s != "":
      if isin:
        s = s.replace("%20"," ")
        s = s.replace("%22","\"")
      try:
        sock.sendto(bytes(sid+s, OutEncoding), (LOX_IP, int(LOX_PORT)))
      except:
        logPrint("<ERROR> sendUDP to "+LOX_IP+":"+LOX_PORT+" im except!")
    if sndlog: sndPrint("UDP: " + UDPstr)

def forwardDictToUDP(url,d_in,fwd_sid,fwd_pwd,status,script,nr,ignoreKeys,remapKeys,sep=" "):
  d = remappedDict(d_in,remapKeys,nr)                          # remap keys in current dictionary
  #d = d_in.copy()                                              # create a separate dict to allow changes there
  outstr = "SID=" + defSID + sep
  okstr=""
  ignoreValues=["-9999","None","null"]
  for key,value in d.items():
    if key in ignoreKeys or (IGNORE_EMPTY and value in ignoreValues):
      None
    elif key == "PASSKEY" and fwd_sid != "":
      outstr += "PASSKEY="+fwd_sid + sep
    elif " " in str(value):
      outstr += key + "=" + "\"" + str(value) + "\"" + sep
    elif key == "loxtime":                                     # add additional unixtime
      outstr += key + "=" + str(value) + sep
      try:
        wert = int(value) + 1230768000 - -time.timezone
        if time.localtime(wert)[8]: wert = wert - 3600
        utime = int(wert) if LOX_TIME else value
        outstr += "unixtime=" + str(utime) + sep
      except ValueError:
        pass
    else:
      outstr += key + "=" + str(value) + sep
  if len(outstr) > 0 and outstr[-1] == sep: outstr = outstr[:-1]
  # add status if requested via FWD_STATUS = True
  if status:
    sw_what = sep+"missed=" + SensorIsMissed if inSensorWarning and SensorIsMissed != "" else ""
    outstr += sep+"running=" + str(int(wsconnected)) + sep + "wswarning=" + str(int(inWStimeoutWarning)) + sep + "sensorwarning=" + str(int(inSensorWarning)) + sw_what + sep + "batterywarning=" + str(int(inBatteryWarning)) + sep + "stormwarning=" + str(int(inStormWarning)) + sep + "tswarning=" + str(int(inTSWarning)) + sep + "updatewarning=" + str(int(updateWarning)) + sep + "leakwarning=" + str(int(inLeakageWarning)) + sep + "co2warning=" + str(int(inCO2Warning)) + sep + "intvlwarning=" + str(int(inIntervalWarning))
  if script != "":
    outstr = modExec(nr, script, outstr)                       # modify outstr with external script before sending
    if outstr == execOnly: return                              # just run the exec-script but do not forward the string
  # addr und port trennen
  addr = url.split(":",1)
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
  sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
  try:
    #sock.sendto(bytes(outstr, "ISO-8859-1"), (addr[0], int(addr[1])))
    sock.sendto(bytes(outstr, OutEncoding), (addr[0], int(addr[1])))
    ret = "OK"
  except socket.error as err:
    ret = str(err.args[0]) + " : " +err.args[1]
    okstr = "<ERROR> "
    pass
  if sndlog: sndPrint(okstr + "FWD-"+nr+ ": " + url + " UDP: " + outstr + " : " + ret)
  return

def forwardStringToUDP(url,payload,fwd_sid,fwd_pwd,status,script,nr,ignoreKeys,remapKeys):
  # sendet eingehenden String payload separiert mit sep per UDP an addr:port (url)
  # used by RAWUDP
  #d = stringToDict(payload,"&")
  d = remappedDict(stringToDict(payload,"&"),remapKeys,nr)     # remap keys in current dictionary
  if status: d.update(addStatusToDict(d, True))                # append status to the dict d if set
  outstr = ""
  okstr = ""
  ignoreValues=["-9999","None","null"]
  for key,value in d.items():
    if key in ignoreKeys or (IGNORE_EMPTY and value in ignoreValues):
      None
    else:
      outstr += key + "=" + str(value) + "&"
  if len(outstr) > 0 and outstr[-1] == "&": outstr = outstr[:-1]
  if script != "":
    outstr = modExec(nr, script, outstr)                       # modify outstr with external script before sending
    if outstr == execOnly: return                              # just run the exec-script but do not forward the string
  # addr und port trennen
  addr = url.split(":",1)
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
  sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
  try:
    #sock.sendto(bytes(outstr, "ISO-8859-1"), (addr[0], int(addr[1])))
    sock.sendto(bytes(outstr, OutEncoding), (addr[0], int(addr[1])))
    ret = "OK"
  except socket.error as err:
    ret = str(err.args[0]) + " : " +err.args[1]
    okstr = "<ERROR> "
    pass
  if sndlog: sndPrint(okstr + "FWD-"+nr+": " + url + " UDP: " + outstr + " : " + ret)
  return

def forwardStringToWU(url,payload,fwd_sid,fwd_pwd,status,script,nr,ignoreKeys,remapKeys):
  # wandelt eingehenden String payload ins WU-Format und versendet url per get
  # Achtung! WOW arbeitet offenbar nicht mit ID/PASSWORD sondern mit siteid und siteAuthenticationKey; bei Windy muss der API-Key direkt in der URL angegeben werden
  #if not "ID=" in url and not "PASSWORD=" in url: url += "?ID="+str(fwd_sid)+"&PASSWORD="+str(fwd_pwd) + "&"
  #if not "action=" in url: url += "action=updateraw" + "&"
  #d = stringToDict(payload,"&")
  d = remappedDict(stringToDict(payload,"&"),remapKeys,nr)     # remap keys in current dictionary
  if status: d.update(addStatusToDict(d, True))                # append status to the dict d if set
  outstr = ""
  dontuse = ("PASSKEY","PASSWORD","ID","model","freq")
  ignoreValues=["-9999","None","null"]
  for key,value in d.items():
    if key in ignoreKeys or key in dontuse or (IGNORE_EMPTY and value in ignoreValues):
      None
    elif key == "stationtype":
      outstr += "softwaretype=" + str(value) + "&"
    elif key == "tempinf":
      outstr += "indoortempf=" + str(value) + "&"
    elif key == "humidityin":
      outstr += "indoorhumidity=" + str(value) + "&"
    elif key == "baromrelin":
      outstr += "baromin=" + str(value) + "&"
    #elif key == "heatindexf":
    #  outstr += "heatIndex=" + str(value) + "&"
    #elif key == "dewptf":
    #  outstr += "dewpt=" + str(value) + "&"
    #elif key == "windchillf":
    #  outstr += "windChill=" + str(value) + "&"
    #elif key == "solarradiation":
    #  outstr += "solarRadiation=" + str(value) + "&"
    #elif key == "feelslikef":
    #  outstr += "feelslike=" + str(value) + "&"
    # neu ab v0.06 - vgl. https://support.weather.com/s/article/PWS-Upload-Protocol?language=en_US
    elif key == "hourlyrainin":                                # could be rainratein instead
      outstr += "rainin=" + str(value) + "&"
    elif key == "dailyrainin":
      outstr += "dailyrainin=" + str(value) + "&"
    # neu ab v0.05 - Awekas akzeptiert nur UV statt uv
    elif key == "uv":
      outstr += "UV=" + str(value) + "&"
    # neu ab v0.06 - Umwandlung von Ecowitt PM2.5 nach WU
    elif key == "pm25_ch1" or key == "pm25":
      outstr += "AqPM2.5=" + str(value) + "&"
    # und PM10 ebenso
    elif key == "pm10_ch1" or key == "pm10":
      outstr += "AqPM10=" + str(value) + "&"
    # WU erwartet soilmoisture statt soilmoisture1
    elif key == "soilmoisture1" or key == "soilhum1":
      outstr += "soilmoisture=" + str(value) + "&"
    # WU erwartet soilbatt statt soilbatt1 (wenn ueberhaupt)
    elif key == "soilbatt1" or key == "battsm1":
      outstr += "soilbatt=" + str(value) + "&"
    # 2do: Ambient-conversion
    elif "soilhum" in key:
      outstr += key.replace("soilhum","soilmoisture") + "=" + str(value) + "&"
    # v0.07: WN35-compatibility
    elif "leafwetness_ch" in key and len(key) == 15:
      outstr += "leafwetness=" + str(value) + "&" if key[-1] == "1" else "leafwetness" + key[-1] + "=" + str(value) + "&"
    # v0.09: WN34-compatibility
    elif "tf_ch" in key and len(key) == 6:
      outstr += "soiltempf=" + str(value) + "&" if key[-1] == "1" else "soiltemp" + key[-1] + "f" + "=" + str(value) + "&"
    else:                                                      # all other values will be sent as present
      outstr += key + "=" + str(value) + "&"
  if len(outstr) > 0 and outstr[-1] == "&": outstr = outstr[:-1]
  if script != "":
    outstr = modExec(nr, script, outstr)                       # modify outstr with external script before sending
    if outstr == execOnly: return                              # just run the exec-script but do not forward the string
  ret = ""
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      r = requests.get(url+outstr,timeout=httpTimeOut)
      ret = str(r.status_code)
      okstr = "<ERROR> " if r.status_code not in range(200,203) else ""
      if r.status_code in range(400,500): v = 400
    except requests.exceptions.Timeout as err:
      ret = "TIMEOUT"
    except requests.exceptions.ConnectionError as err:
      ret = "CONNERR"
    except requests.exceptions.RequestException as err:
      ret = "REQERR"
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  if sndlog: sndPrint(okstr + "FWD-"+nr+": " + url + outstr + " : " + ret + tries)
  return

def dictToEW(d_in,fwd_sid,fwd_pwd,status,script,nr,ignoreKeys,remapKeys):
  d = remappedDict(d_in,remapKeys,nr)                          # remap keys in current dictionary
  #d = d_in.copy()
  isAmbientWeather = checkAmbientWeather(d)
  outstr = ""
  dontuse = ("ID","PASSWORD","action","realtime","rtfreq","MAC")
  ignoreValues=["-9999","None","null"]
  if status: d.update(addStatusToDict(d, True))                # append status to the dict d if set
  for key,value in d.items():
    if key in ignoreKeys or key in dontuse or (IGNORE_EMPTY and value in ignoreValues):
      None
    # 2do: possibility to exchange the PASSKEY
    elif key == "PASSKEY" and fwd_sid != "":
      outstr += "PASSKEY="+fwd_sid + "&"
    elif key == "dateutc":
      if value == "now":                                       # wenn "now" dann aktuelle Zeit, ansonsten Unixtime in Datumsstring wandeln
        isnow = time.strftime("%Y-%m-%d+%H:%M:%S",time.gmtime())
        value = time.strptime(isnow, "%Y-%m-%d+%H:%M:%S")
      else: value = value.replace("%20","+").replace("%3A",":")
      outstr += "dateutc=" +  str(value) + "&"
    elif key == "rainin" and not "hourlyrainin" in d:
      outstr += "hourlyrainin=" + str(value) + "&"
    elif key == "UV":
      outstr += "uv=" + str(value) + "&"
    elif key == "indoortempf":
      outstr += "tempinf=" + str(value) + "&"
    elif key == "indoorhumidity":
      outstr += "humidityin=" + str(value) + "&"
    # v0.09: for WH6006 compatibility
    elif key == "baromin" or key == "barominrelin":
      outstr += "baromrelin=" + str(value) + "&"
    elif key == "absbaro":
      outstr += "baromabsin=" + str(value) + "&"
    elif key == "heatIndex":
      outstr += "heatindexf=" + str(value) + "&"
    elif key == "dewpt":
      outstr += "dewptf=" + str(value) + "&"
    elif key == "windchill":
      outstr += "windChillf=" + str(value) + "&"
    elif key == "solarRadiation":
      outstr += "solarradiation=" + str(value) + "&"
    elif key == "feelslike":
      outstr += "feelslikef=" + str(value) + "&"
    elif key == "softwaretype":
      outstr += "stationtype=" + str(value) + "&"
    elif key == "AqPM2.5":
      outstr += "pm25_ch1=" + str(value) + "&"
    elif key == "AqCO2":
      outstr += "co2=" + str(value) + "&"
    # Ambient-specific keys
    elif "soilhum" in key:
      outstr += key.replace("soilhum","soilmoisture") + "=" + str(value) + "&"
    # if coming from WU format
    elif key == "soilmoisture":
      outstr += "soilmoisture1=" + str(value) + "&"
    elif key == "soilbatt":
      outstr += "soilbatt1=" + str(value) + "&"
    elif key == "leafwetness":
      outstr += "leafwetness_ch1=" + str(value) + "&"
    elif "leafwetness" in key and len(key) == 12:
      outstr += "leafwetness_ch" + key[-1] + "=" + str(value) + "&"
    elif "leak" in key and len(key) == 5:
      outstr += "leak_ch" + key[-1] + "=" + str(value) + "&"
    elif key == "lightning_day":
      outstr += "lightning_num=" + str(value) + "&"
    elif key == "lightning_distance":
      outstr += "lightning=" + str(value) + "&"
    elif key == "pm25":
      outstr += "pm25_ch1=" + str(value) + "&"
    elif key == "pm25_24h":
      outstr += "pm25_avg_24h_ch1=" + str(value) + "&"
    elif key == "pm10_in_aqin":
      outstr += "pm10_co2=" + str(value) + "&"
    elif key == "pm10_in_24h_aqin":
      outstr += "pm10_24h_co2=" + str(value) + "&"
    elif key == "pm25_in_aqin":
      outstr += "pm25_co2=" + str(value) + "&"
    elif key == "pm25_in_24h_aqin":
      outstr += "pm25_24h_co2=" + str(value) + "&"
    elif key == "co2_in_aqin":
      outstr += "co2=" + str(value) + "&"
    elif key == "co2_in_24h_aqin":
      outstr += "co2_24h=" + str(value) + "&"
    elif key == "pm_in_temp_aqin":
      outstr += "tf_co2=" + str(value) + "&"
    elif key == "pm_in_humidity_aqin":
      outstr += "humi_co2=" + str(value) + "&"
    elif key == "totalrainin" or key == "totalrain":
      outstr += "totalrainin="  + str(value) + "&"
      if not "yearlyrainin" in d:
        outstr += "yearlyrainin=" + str(value) + "&"
    # 2do: Ambient battery values needs to be transformed too! 1 = ok; 0 = warning-state
    # still open: batt_25in, battrN
    elif key == "battout":
      if isAmbientWeather:
        battval = 0 if int(value) >= 1 else 1
      else:
        battval = value
      outstr += "wh65batt=" + str(battval) + "&"
    elif key == "battin":
      if isAmbientWeather:
        battval = 0 if int(value) >= 1 else 1
      else:
        battval = value
      outstr += "battin=" + str(battval) + "&"
    elif key == "batt_25":
      battval = 0 if int(value) >= 1 else 3
      outstr += "pm25batt1=" + str(battval) + "&"
    elif key == "battrain":
      battval = 1.2 if int(value) >= 1 else 1.3
      outstr += "wh40batt=" + str(battval) + "&"
    elif key == "batt_lightning":
      battval = 0 if int(value) >= 1 else 3
      outstr += "wh57batt=" + str(battval) + "&"
    elif "batleak" in key:
      battval = 0 if int(value) >= 1 else 3
      outstr += key.replace("batleak","leakbatt") + "=" + str(battval) + "&"
    elif "battsm" in key:
      battval = 1.2 if int(value) >= 1 else 1.3
      outstr += key.replace("battsm","soilbatt") + "=" + str(battval) + "&"
    elif "batt" in key and len(key) == 5:
      if isAmbientWeather:
        battval = 0 if int(value) >= 1 else 1
      else:
        battval = value
      outstr += "batt" + key[-1] + "=" + str(battval) + "&"
    elif key == "batt_co2":
      battval = 0 if int(value) >= 1 else 3                    # which value sends AW if powered through USB?
      outstr += "co2_batt=" + str(battval) + "&"
    else:
      outstr += key + "=" + str(value) + "&"
  if len(outstr) > 0 and outstr[-1] == "&": outstr = outstr[:-1]
  # exec script to modify the outgoing string
  if script != "": outstr = modExec(nr, script, outstr)        # modify outstr with external script before sending
  return outstr

def forwardStringToEW(url,payload,fwd_sid,fwd_pwd,status,script,nr,ignoreKeys,remapKeys):
  # wandelt eingehenden String payload ins Ecowitt-Format und versendet url per post
  # wenn kein PASSKEY vorhanden, setzen!
  d = stringToDict(payload,"&")                                # remap will be done in dictToEW
  outstr = dictToEW(d,fwd_sid,fwd_pwd,status,script,nr,ignoreKeys,remapKeys)
  if script != "" and outstr == execOnly: return               # just run the exec-script but do not forward the string
  ret = ""
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Connection': 'Close','User-Agent': None}
      #headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Connection': 'Close', 'User-Agent': None, 'Accept': None, 'Accept-Encoding': None}
      #r = requests.put(url,data=outstr)
      r = requests.post(url,data=outstr,headers=headers,timeout=httpTimeOut)
      ret = str(r.status_code)
      okstr = "<ERROR> " if r.status_code not in range(200,203) else ""
      if r.status_code in range(400,500): v = 400
    except requests.exceptions.Timeout as err:
      ret = "TIMEOUT"
    except requests.exceptions.ConnectionError as err:
      ret = "CONNERR"
    except requests.exceptions.RequestException as err:
      ret = "REQERR"
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  if sndlog: sndPrint(okstr + "FWD-"+nr+": " + url + " post: " + outstr + " : " + ret + tries)
  return

def convBattToAMB(key, value):
  # output: low battery = 0; normal = 1
  battok = "0"
  if ("wh65batt" in key or "lowbatt" in key or "wh26batt" in key or "wh25batt" in key) and int(value) == 0 : battok = "1"
  elif "batt" in key and len(key) == 5 and int(value) == 0: battok = "1"
  elif ("wh57batt" in key or "pm25batt" in key or "leakbatt" in key or "co2_batt" in key) and int(value) >= 2: battok = "1"
  elif ("soilbatt" in key or "wh40batt" in key or "wh68batt" in key or "leaf_batt" in key or "tf_batt" in key) and float(value) > 1.2: battok = "1"
  elif ("wh80batt" in key or "wh90batt" in key) and float(value) > 2.3: battok = "1"
  return battok

def forwardStringToAMB(url,payload,fwd_sid,fwd_pwd,status,script,nr,ignoreKeys,remapKeys):
  # based on https://help.ambientweather.net/help/advanced/
  # 2do: not yet extensively tested!
  # wandelt eingehenden String payload ins Ambient-Format und versendet url per GET
  # wenn keine MAC vorhanden, setzen!
  # in contrast to the description, it is sent via GET; PASSKEY is the original PASSKEY from Ecowitt (when ordering VW-ANET, the original MAC address of the Ecowitt device must be given!)
  # https://www.ambientweather.com/amwevwamweac.html
  #d = stringToDict(payload,"&")
  d = remappedDict(stringToDict(payload,"&"),remapKeys,nr)     # remap keys in current dictionary
  if status: d.update(addStatusToDict(d, True))                # append status to the dict d if set
  isAmbientWeather = checkAmbientWeather(d)
  outstr = ""
  ignoreValues=["-9999","None","null"]
  dontuse = ("ID","PASSWORD","action","realtime","rtfreq")
  for key,value in d.items():
    if key in ignoreKeys or key in dontuse or (IGNORE_EMPTY and value in ignoreValues):
      None
    # 2do: possibility to exchange the MAC - seems to be PASSKEY instead of MAC
    #elif key == "MAC" and fwd_sid != "":
    #  outstr += "MAC="+fwd_sid + "&"
    elif key == "PASSKEY" and fwd_sid != "":
      outstr += "PASSKEY="+fwd_sid + "&"
    elif key == "dateutc":
      # wenn "now" dann aktuelle Zeit, ansonsten Unixtime in Datumsstring wandeln
      if value == "now":                                       # if source is e.g. WU
        value = time.strftime("%Y-%m-%d+%H:%M:%S",time.gmtime())
      # if date and time separated by "+"
      outstr += "dateutc=" +  str(value.replace("%20","+").replace("%3A",":")) + "&"
    elif key == "rainin" and not "hourlyrainin" in d:
      outstr += "hourlyrainin=" + str(value) + "&"
    elif key == "UV":
      outstr += "uv=" + str(value) + "&"
    elif key == "indoortempf":
      outstr += "tempinf=" + str(value) + "&"
    elif key == "indoorhumidity":
      outstr += "humidityin=" + str(value) + "&"
    elif key == "baromin":
      outstr += "barominrelin=" + str(value) + "&"
    elif key == "heatIndex":
      outstr += "heatindexf=" + str(value) + "&"
    elif key == "dewpt":
      outstr += "dewptf=" + str(value) + "&"
    elif key == "windchill":
      outstr += "windChillf=" + str(value) + "&"
    elif key == "solarRadiation":
      outstr += "solarradiation=" + str(value) + "&"
    elif key == "feelslike":
      outstr += "feelslikef=" + str(value) + "&"
    elif key == "softwaretype":
      outstr += "stationtype=" + str(value) + "&"
    # exchange some keys with Ambient-keys
    elif "soilmoisture" in key:
      outstr += key.replace("soilmoisture","soilhum")  + "=" + str(value) + "&"
    elif "leak_ch" in key:
      outstr += key.replace("leak_ch","leak")  + "=" + str(value) + "&"
    elif key == "lightning_num":
      outstr += "lightning_day=" + str(value) + "&"
    elif key == "lightning":
      outstr += "lightning_distance=" + str(value) + "&"
    #elif key == "totalrainin":
    #  outstr += "totalrain=" + str(value) + "&"
    # Ambient only accepts one outdoor-PM25 and one indoor-PM25 - we use #1 as outdoor and #2 as indoor
    elif key == "pm25_ch1":
      outstr += "pm25=" + str(value) + "&"
    elif key == "pm25_avg_24h_ch1":
      outstr += "pm25_24h=" + str(value) + "&"
    # Ambient only accepts one outdoor-PM25 and one indoor-PM25 - we use #1 as outdoor and #2 as indoor
    elif key == "pm25_ch2":
      outstr += "pm25_in=" + str(value) + "&"
    elif key == "pm25_avg_24h_ch2":
      outstr += "pm25_in_24h=" + str(value) + "&"
    # v0.09: WH45 compatibility - should work with v4.32 now
    elif key == "tf_co2":
      outstr += "pm_in_temp_aqin=" + str(value) + "&"
      #outstr += "tempf_co2=" + str(value) + "&"
    elif key == "humi_co2":
      outstr += "pm_in_humidity_aqin=" + str(value) + "&"
      #outstr += "humidity_co2=" + str(value) + "&"
    elif key == "pm10_co2":
      outstr += "pm10_in_aqin=" + str(value) + "&"
    elif key == "pm10_24h_co2":
      outstr += "pm10_in_24h_aqin=" + str(value) + "&"
    elif key == "pm25_co2":
      outstr += "pm25_in_aqin=" + str(value) + "&"
    elif key == "pm25_24h_co2":
      outstr += "pm25_in_24h_aqin=" + str(value) + "&"
    elif key == "co2":
      outstr += "co2_in_aqin=" + str(value) + "&"
    elif key == "co2_24h":
      outstr += "co2_in_24h_aqin=" + str(value) + "&"
    # v0.09: WN34 compatibility
    elif "tf_ch" in key:
      outstr += key.replace("tf_ch","soiltemp")  + "=" + str(value) + "&"
    # 2do exchange battery-values - values have to be interpreted
    # v0.09: added wh80batt
    elif key == "wh65batt" or key == "wh80batt" or key == "wh90batt":
      outstr += "battout=" + convBattToAMB(key, value) + "&"
    elif "batt" in key and len(key) == 5:
      if isAmbientWeather:
        outstr += "batt" + key[-1] + "=" + str(value) + "&"
      else:
        outstr += "batt" + key[-1] + "=" + convBattToAMB(key, value) + "&"
    # Ambient only accepts one outdoor-PM25 and one indoor-PM25 - we use #1 as outdoor and #2 as indoor
    elif key == "pm25batt1":
      outstr += "batt_25=" + convBattToAMB(key, value) + "&"
    # Ambient only accepts one outdoor-PM25 and one indoor-PM25 - we use #1 as outdoor and #2 as indoor
    elif key == "pm25batt2":
      outstr += "batt_25in=" + convBattToAMB(key, value) + "&"
    elif key == "wh40batt":
      outstr += "battrain=" + convBattToAMB(key, value) + "&"
    elif key == "wh57batt":
      # acc. docs it should be 0 for low and 1 for ok - but with v4.2.9 and server configuration at 21.03.21 this does not work as expected
      #outstr += "batt_lightning=" + convBattToAMB(key, value) + "&"
      outstr += "batt_lightning=" + str(value) + "&"
    elif "leakbatt" in key:
      amb_battkey = key
      # acc. docs it should be 0 for low and 1 for ok - but with v4.2.9 and server configuration at 21.03.21 this does not work as expected
      #outstr += amb_battkey.replace("leakbatt","batleak")  + "=" + convBattToAMB(key, value) + "&"
      outstr += amb_battkey.replace("leakbatt","batleak")  + "=" + str(value) + "&"
    elif "soilbatt" in key:
      amb_battkey = key
      outstr += amb_battkey.replace("soilbatt","battsm")  + "=" + convBattToAMB(key, value) + "&"
    # v0.08 hopefully right:
    elif "tf_batt" in key:
      amb_battkey = key
      outstr += amb_battkey.replace("tf_batt","batt_tf")  + "=" + convBattToAMB(key, value) + "&"
    elif "co2_batt" in key:
      amb_battkey = key
      outstr += amb_battkey.replace("co2_batt","batt_co2")  + "=" + convBattToAMB(key, value) + "&"
    elif key == "wh25batt":
      outstr += "battin=" + convBattToAMB(key, value) + "&"
    elif key == "wh26batt":
      outstr += "battout=" + convBattToAMB(key, value) + "&"
    else:
      outstr += key + "=" + str(value) + "&"
  if len(outstr) > 0 and outstr[-1] == "&": outstr = outstr[:-1]
  if script != "":
    outstr = modExec(nr, script, outstr)                       # modify outstr with external script before sending
    if outstr == execOnly: return                              # just run the exec-script but do not forward the string
  ret = ""
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Connection': 'Close','User-Agent': None}
      # for now Ambient will sent via GET instead of POST
      r = requests.get(url+outstr,headers=headers,timeout=httpTimeOut)
      # Ambient responds 200 in any case - so additionally we have to check for OK
      ret = str(r.text) if r.status_code in range(200,203) else str(r.status_code)
      okstr = "<ERROR> " if r.status_code not in range(200,203) or ret != "OK" else ""
      if r.status_code in range(400,500): v = 400
    except requests.exceptions.Timeout as err:
      ret = "TIMEOUT"
    except requests.exceptions.ConnectionError as err:
      ret = "CONNERR"
    except requests.exceptions.RequestException as err:
      ret = "REQERR"
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  if sndlog: sndPrint(okstr + "FWD-"+nr+": " + url + outstr + " : " + ret + tries)
  return

def forwardDictToHTTP(url,d_in,fwd_sid,fwd_pwd,status,script,nr,ignoreKeys,remapKeys,ecowitt=False,hideSpace=False,withSID=True,sep="&"):
  # uebergebenes dict d (metrisch oder imperial) als String zusammensetzen und an url per get oder put/post (Ecowitt) versenden
  # RAW, RAWCSV, CSV, unknown - POST oder GET je nach ecowitt=True/False
  d = remappedDict(d_in,remapKeys,nr)                          # remap keys in current dictionary
  #d = d_in.copy()                                              # create a separate dict to allow changes there
  outstr = "SID=" + defSID + sep if withSID else ""
  dontuse = () if ecowitt else ("PASSKEY","PASSWORD","ID","model","freq")
  for key,value in d.items():
    if key in ignoreKeys or key in dontuse:
      None
    else:
      outstr += key + "=" + str(value).replace(" ","%20") + sep if hideSpace else key + "=" + str(value) + sep
  if len(outstr) > 0 and outstr[-1] == sep: outstr = outstr[:-1]
  if script != "":
    outstr = modExec(nr, script, outstr)                       # modify outstr with external script before sending
    if outstr == execOnly: return                              # just run the exec-script but do not forward the string
  ret = ""
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      #r = requests.put(url,data=outstr) if ecowitt else requests.get(url+outstr)
      r = requests.post(url,data=outstr,timeout=httpTimeOut) if ecowitt else requests.get(url+outstr,timeout=httpTimeOut)
      ret = str(r.status_code)
      okstr = "<ERROR> " if r.status_code not in range(200,203) else ""
      if r.status_code in range(400,500): v = 400
    except requests.exceptions.Timeout as err:
      ret = "TIMEOUT"
    except requests.exceptions.ConnectionError as err:
      ret = "CONNERR"
    except requests.exceptions.RequestException as err:
      ret = "REQERR"
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  if sndlog: sndPrint(okstr + "FWD-"+nr+": " + url + outstr + " : " + ret + tries)
  return

def getfromDict(d, a, ignoreKeys = {}):
  outstr = "null"
  for i in range(len(a)):
    if a[i] in d and a[i] not in ignoreKeys:
      outstr = d[a[i]]
      break
  return outstr

def localWUTimeString(s):
  if len(s) == 19:
    wert = int(time.mktime(time.strptime(s, "%Y-%m-%d+%H:%M:%S")))-time.timezone
  else:
    wert = int(time.time())
  if time.localtime(wert)[8]: wert + 3600
  s = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime(wert))
  return s

def utcWUTimeString(s):
  if len(s) == 19:
    wert = int(time.mktime(time.strptime(s, "%Y-%m-%d+%H:%M:%S")))
  else:
    wert = int(time.time())+time.timezone
  s = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime(wert))
  return s

def dictToWUServer(d, sep, metric):
  s = ""
  if d != "":
    s = "{\"observations\":[{"
    stid = getfromDict(d,["stationID"])
    if stid == "" or stid == "null": stid = prgname
    s += "\"stationID\":\""+stid+"\","
    s += "\"obsTimeUtc\":\""+ utcWUTimeString(getfromDict(d,["dateutc","obsTimeUtc"]))+"\","
    s += "\"obsTimeLocal\":\""+ localWUTimeString(getfromDict(d,["obsTimeLocal"]))+"\","
    s += "\"neighborhood\":\""+ getfromDict(d,["neighborhood"])+"\","
    s += "\"softwareType\":\""+ getfromDict(d,["softwareType","softwaretype"])+"\","
    s += "\"country\":\""+ getfromDict(d,["country"])+"\","
    s += "\"solarradiation\":"+ getfromDict(d,["solarradiation","solarRadiation"])+","
    lon = getfromDict(d,["lon"])
    if lon == "null" and COORD_LON != "": lon = COORD_LON      # exchange defaults with given parameters
    s += "\"lon\":"+lon+","
    lat = getfromDict(d,["lat"])
    if lat == "null" and COORD_LAT != "": lat = COORD_LAT      # exchange defaults with given parameters
    s += "\"lat\":"+lat+","
    s += "\"realtimeFrequency\":"+ getfromDict(d,["realtimeFrequency","rtfreq"])+","
    try:
      epoch = str(int(time.mktime(time.strptime(getfromDict(d,["dateutc","obsTimeUtc"]), "%Y-%m-%d+%H:%M:%S"))-time.timezone))
    except ValueError:
      epoch = "null"
      pass
    s += "\"epoch\":"+ epoch +","
    # neu ab v0.05 - Awekas akzeptiert uv nur in Grossbuchstaben
    s += "\"UV\":"+ getfromDict(d,["uv","UV"])+","
    s += "\"winddir\":"+ getfromDict(d,["winddir"])+","
    s += "\"humidity\":"+ getfromDict(d,["humidity","humidityin"])+","
    # v0.08: additional temp/hum sensors WH31
    for i in range(1,9):
      try:
        i_s = str(i)
        hum = getfromDict(d,["humidity"+i_s])
        if hum != "null":
          s += "\"humidity" + i_s + "\":"+ hum + ","
      except ValueError: pass
    # ab v0.06 - PM2.5 for channel 1 only; Ambient uses pm25
    wert = getfromDict(d,["pm25_ch1","AqPM2.5","pm25"])
    if wert != "null":
      #s += "\"AqPM2.5\":"+ getfromDict(d,["pm25_ch1","AqPM2.5"]) + ","
      s += "\"AqPM2.5\":"+ wert + ","
    # same for PM10
    wert = getfromDict(d,["pm10_ch1","AqPM10"])
    if wert != "null":
      #s += "\"AqPM10\":"+ getfromDict(d,["pm10_ch1","AqPM10"]) + ","
      s += "\"AqPM10\":"+ wert + ","
    s += "\"qcStatus\":"+ getfromDict(d,["qcStatus"])+","
    for i in range(1,9):
      try:
        i_s = str(i)
        # for Ambient compatibility
        soil = getfromDict(d,["soilmoisture"+i_s,"soilhum"+i_s])
        if soil != "null":
          if i == 1:
            s += "\"soilmoisture\":"+ soil +","
          else:
            s += "\"soilmoisture" + i_s + "\":"+ soil + ","
      except ValueError: pass
    # v0.09: WN35
    for i in range(1,9):
      try:
        i_s = str(i)
        leaf = getfromDict(d,["leafwetness_ch"+i_s])
        if leaf != "null":
          if i == 1:
            s += "\"leafwetness\":"+ leaf +","
          else:
            s += "\"leafwetness" + i_s + "\":"+ leaf + ","
      except ValueError: pass
    if metric:                                   # metrische Daten
      s += "\"metric\":"
    else:                                        # imperial
      s += "\"imperial\":"
    s += "{"
    s += "\"temp\":"+ getfromDict(d,["temp","tempf","tempc"])+","
    # v0.08: additional temp/hum sensors WH31
    e_char = "c" if metric else "f"
    for i in range(1,9):
      try:
        i_s = str(i)
        temp = getfromDict(d,["temp"+i_s+e_char])
        if temp != "null":
          s += "\"temp" + i_s + "f" + "\":"+ temp + ","
      except ValueError: pass
    # v0.09: WN34
    e_char = "c" if metric else ""
    for i in range(1,9):
      try:
        i_s = str(i)
        temp = getfromDict(d,["tf_ch"+i_s+e_char])
        if temp != "null":
          if i_s == "1":
            s += "\"soiltemp" + "f" + "\":"+ temp + ","
          else:
            s += "\"soiltemp" + i_s + "f" + "\":"+ temp + ","
      except ValueError: pass
    s += "\"heatIndex\":"+ getfromDict(d,["heatindexf","heatindexc"])+","
    s += "\"dewpt\":"+ getfromDict(d,["dewpt","dewptf","dewptc"])+","
    s += "\"windChill\":"+ getfromDict(d,["windChill","windchillf","windchillc"])+","
    s += "\"windSpeed\":"+ getfromDict(d,["windSpeed","windspeedmph","windspeedkmh"])+","
    s += "\"windGust\":"+ getfromDict(d,["windGust","windgustmph","windgustkmh"])+","
    s += "\"pressure\":"+ getfromDict(d,["pressure","baromrelin","baromrelhpa","baromhpa","baromin"])+","
    s += "\"precipRate\":"+ getfromDict(d,["precipRate","hourlyrainin","hourlyrainmm","rainmm"])+","
    s += "\"precipTotal\":"+ getfromDict(d,["precipTotal","dailyrainin","dailyrainmm"])+","
    s += "\"elev\":"+ getfromDict(d,["elev"])+""
    s += "}"
    s += "}]}"
  return s

def remappedDict(d_in,remapKeys,nr):
  d = d_in.copy()
  for key, value in remapKeys.items():
    try:
      newval = last_d_all[value[1:]] if value[0] == "@" else value      # use value of dict if @ given
      if key[0] == "@" and last_d_all[key[1:]]: key = key[1:]           # make sure key exist with @
      if newval == "-": d.pop(key,None)                                 # delete the key if value is "-"
      else: d.update({key : newval})                                    # update the dict with key:value
    except:
      if nr != "": sndPrint("<ERROR> FWD-"+nr+": Remap: problem while remapping " + key + " with " + value, True)
      pass
  return d

def forwardDictToAwekas(url,d_in,fwd_sid,fwd_pwd,script,nr,ignoreKeys,remapKeys):
  # use API to upload data to Awekas
  d = remappedDict(d_in,remapKeys,nr)                          # remap keys in current dictionary
  ignoreValues=["-9999","None","null",""]
  outstr = ""
  # Awekas only needs the MD5-hash of password
  fwd_pwd = hashlib.md5(fwd_pwd.encode('utf-8')).hexdigest()
  value = getfromDict(d,["tempinc"],ignoreKeys)
  if not (IGNORE_EMPTY and value in ignoreValues): outstr += "indoortemp=" + str(value) + "&"

  value = getfromDict(d,["humidityin"],ignoreKeys)
  if not (IGNORE_EMPTY and value in ignoreValues): outstr += "indoorhumidity=" + str(value) + "&"

  # for all soil temp sensors (1..8)
  for i in range(1,9):
    try:
      i_s = str(i)
      value = getfromDict(d,["tf_ch"+i_s+"c"],ignoreKeys)
      if not (IGNORE_EMPTY and value in ignoreValues): outstr += "soiltemp"+i_s+"=" + str(value) + "&"
    except ValueError: pass

  ## for all WH31 temp sensors (1..8) - not supported by Awekas yet
  #for i in range(1,9):
  #  try:
  #    i_s = str(i)
  #    value = getfromDict(d,["temp"+i_s+"c"],ignoreKeys)
  #    if not (IGNORE_EMPTY and value in ignoreValues): outstr += "temp"+i_s+"=" + str(value) + "&"
  #  except ValueError: pass

  # for all soil moisture sensors (1..8)
  for i in range(1,9):
    try:
      i_s = str(i)
      value = getfromDict(d,["soilmoisture"+i_s,"soilhum"+i_s],ignoreKeys)
      if not (IGNORE_EMPTY and value in ignoreValues): outstr += "soilmoisture"+i_s+"=" + str(value) + "&"
    except ValueError: pass

  # for all leaf wetness sensors (1..8)
  for i in range(1,9):
    try:
      i_s = str(i)
      value = getfromDict(d,["leafwetness_ch"+i_s,"leafwet"+i_s],ignoreKeys)
      if not (IGNORE_EMPTY and value in ignoreValues): outstr += "leafwetness"+i_s+"=" + str(leafTo15(value)) + "&"
    except ValueError: pass

  # for all WH31 sensors (1..8)
  for i in range(1,9):
    try:
      i_s = str(i)
      value = getfromDict(d,["humidity"+i_s],ignoreKeys)
      if not (IGNORE_EMPTY and value in ignoreValues): outstr += "hum"+i_s+"=" + str(value) + "&"
    except ValueError: pass

  # for the first WH41
  value = getfromDict(d,["pm25_ch1","pm25"],ignoreKeys)
  if not (IGNORE_EMPTY and value in ignoreValues): outstr += "AqPM2.5=" + str(value) + "&"
  value = getfromDict(d,["pm25_avg_24h_ch1","pm25_24h"],ignoreKeys)
  if not (IGNORE_EMPTY and value in ignoreValues): outstr += "AqPM2.5_avg_24h=" + str(value) + "&"
  # define reply format
  outstr += "output=text" + "&" 

  #################################################################################################
  # some more data - without keynames - order is fixed - separated by ";"
  outstr += "val=" + fwd_sid + ";" + fwd_pwd + ";"

  # date & time
  value = getfromDict(d,["dateutc"],ignoreKeys)
  if not (IGNORE_EMPTY and value in ignoreValues):
    try:
      value = value.replace("%20","+").replace("%3A",":")
      isdate = value[8:10] + "." + value[5:7] + "." + value[0:4]
      # time in UTC or local time? - now UTC:
      istime = value[11:13] + ":" + value[14:16]
    except ValueError:
      isnow = time.gmtime()
      isdate = time.strftime('%d.%m.%Y', isnow)
      istime = time.strftime('%H:%M', isnow)
  else:
    isnow = time.gmtime()
    isdate = time.strftime('%d.%m.%Y', isnow)
    istime = time.strftime('%H:%M', isnow)
  outstr += isdate + ";" + istime + ";"

  value = getfromDict(d,["tempc"],ignoreKeys)
  outstr += str(value) + ";" if not (IGNORE_EMPTY and value in ignoreValues) else ";"

  value = getfromDict(d,["humidity"],ignoreKeys)
  outstr += str(value) + ";" if not (IGNORE_EMPTY and value in ignoreValues) else ";"

  value = getfromDict(d,["baromrelhpa","baromhpa"],ignoreKeys)
  outstr += str(value) + ";" if not (IGNORE_EMPTY and value in ignoreValues) else ";"

  value = getfromDict(d,["dailyrainmm"],ignoreKeys)
  outstr += str(value) + ";" if not (IGNORE_EMPTY and value in ignoreValues) else ";"

  value = getfromDict(d,["windspeedkmh"],ignoreKeys)
  outstr += str(value) + ";" if not (IGNORE_EMPTY and value in ignoreValues) else ";"

  value = getfromDict(d,["winddir"],ignoreKeys)
  outstr += str(value) + ";" if not (IGNORE_EMPTY and value in ignoreValues) else ";"

  # weather condition, warning condition, snow height, language, tendency
  outstr += ";;;de;;"

  value = getfromDict(d,["windgustkmh"],ignoreKeys)
  outstr += str(value) + ";" if not (IGNORE_EMPTY and value in ignoreValues) else ";"

  value = getfromDict(d,["solarradiation","solarRadiation"],ignoreKeys)
  outstr += str(value) + ";" if not (IGNORE_EMPTY and value in ignoreValues) else ";"

  value = getfromDict(d,["uv","UV"],ignoreKeys)
  outstr += str(value) + ";" if not (IGNORE_EMPTY and value in ignoreValues) else ";"

  # Awekas accepts either solarradiation or brightness but prefers SR
  # in fact Awekas will not work correctly with both sent - deactivated until Awekas has fixed this
  #value = getfromDict(d,["brightness","luminosity"])
  #outstr += str(value) + ";" if not (IGNORE_EMPTY and value in ignoreValues) else ";"
  outstr += ";"

  # sunshine hours
  value = getfromDict(d,["sunhours"],ignoreKeys)
  outstr += str(value) + ";" if not (IGNORE_EMPTY and value in ignoreValues) else ";"

  # soiltemp1 (again?)
  value = getfromDict(d,["tf_ch1c"],ignoreKeys)
  outstr += str(value) + ";" if not (IGNORE_EMPTY and value in ignoreValues) else ";"

  # rain rate
  value = getfromDict(d,["rainratemm"],ignoreKeys)
  outstr += str(value) + ";" if not (IGNORE_EMPTY and value in ignoreValues) else ";"

  # software flag - Awekas supports only 15 char - we have to adjust our name
  swver = prgname + "_" +prgver.replace("v","").replace(".","")
  outstr += swver + ";"
  
  # long/lat
  #outstr += ";;"
  lon = getfromDict(d,["lon"],ignoreKeys)
  if lon == "null": lon = COORD_LON                            # exchange defaults with given parameters
  lat = getfromDict(d,["lat"],ignoreKeys)
  if lat == "null": lat = COORD_LAT                            # exchange defaults with given parameters
  outstr += lon+";"+lat+";"

  # clean outstr
  if len(outstr) > 0 and outstr[-1] == ";": outstr = outstr[:-1]

  if script != "":
    outstr = modExec(nr, script, outstr)                       # modify outstr with external script before sending
    if outstr == execOnly: return                              # just run the exec-script but do not forward the string

  ret = ""
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Connection': 'Close','User-Agent': None}
      # check URL and add needed ?
      if url[-1] != "?": url += "?"
      # Awekas is using http/GET
      r = requests.post(url+outstr,headers=headers,timeout=httpTimeOut)
      # Awekas responds 200 in any case - so additionally we have to check for OK
      ret = str(r.text) if r.status_code in range(200,203) else str(r.status_code)
      okstr = "<ERROR> " if r.status_code not in range(200,203) or ret != "OK" else ""
      if ret != "OK": v = 400
    except requests.exceptions.Timeout as err:
      ret = "TIMEOUT"
    except requests.exceptions.ConnectionError as err:
      ret = "CONNERR"
    except requests.exceptions.RequestException as err:
      ret = "REQERR"
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  if sndlog: sndPrint(okstr + "FWD-"+nr+": " + url + " post: " + outstr + " : " + ret + tries)
  return

def forwardDictToWetterSektor(url,d_in,fwd_sid,fwd_pwd,script,nr,ignoreKeys,remapKeys):
  # convert incoming metric dict to WetterSektor-API via http/POST
  d = remappedDict(d_in,remapKeys,nr)                          # remap keys in current dictionary
  outstr = "?val=" if "?val=" not in url else ""
  now = time.localtime()
  # remap Keys

  outstr += fwd_sid + ";"                                                                # username
  outstr += fwd_pwd + ";"                                                                # password
  outstr += time.strftime('%d.%m.%Y', now) + ";"                                         # Datum(TT.MM.JJJJ)
  outstr += time.strftime('%H:%M', now) + ";"                                            # Uhrzeit(SS:MM)
  outstr = outstr+getfromDict(d,["tempc"],ignoreKeys)+";"                                # Temperatur
  outstr = outstr+getfromDict(d,["baromrelhpa","baromhpa"],ignoreKeys)+";"               # Luftdruck
  outstr = outstr+getfromDict(d,["pchange3"],ignoreKeys)+";"                             # Luftdrucktrend3h 2do: ptrend3 or pchange3? - WSWin: -0.9+
  outstr = outstr+getfromDict(d,["humidity"],ignoreKeys)+";"                             # Luftfeuchte
  outstr = outstr+getfromDict(d,["windspdkmh_avg10m"],ignoreKeys)+";"                    # Wind10min
  outstr = outstr+WindDirText(getfromDict(d,["winddir_avg10m"],ignoreKeys),"XX")+";"     # Windrichtung(10min)InTextform-z.B.N-NO - WSWin: N-NO
  outstr = outstr+getfromDict(d,["maxdailygust"],ignoreKeys)+";"                         # WindspitzeTag
  outstr = outstr+getfromDict(d,["rainratemm"],ignoreKeys)+";"                           # RegenAktuellerDatensatz
  outstr = outstr+getfromDict(d,["hourlyrainmm"],ignoreKeys)+";"                         # Regen1h
  outstr = outstr+getfromDict(d,["dailyrainmm"],ignoreKeys)+";"                          # Regen24h
  outstr += ";"                                                                          # unknown (weather-icon)
  outstr += ";"                                                                          # unknown (weather-value?)
  outstr += ";"                                                                          # Helligkeit%(0=dunkel,100=sonnig)
  outstr = outstr+decHourToHMstr(getfromDict(d,["sunhours"],ignoreKeys))+";"             # Sonnenzeit - WSWin: h:m - also nicht dezimal!
  outstr = outstr+getfromDict(d,["dewptc"],ignoreKeys)+";"                               # Taupunkt
  outstr += ";"                                                                          # Temperaturänderung1h
  outstr = outstr+getfromDict(d,["uv","UV"],ignoreKeys)+";"                              # UV-Index
  outstr = outstr+str(getfromDict(min_max,["tempc_min"],ignoreKeys))+";"                 # TempMinHeute
  outstr = outstr+str(getfromDict(min_max,["tempc_max"],ignoreKeys))+";"                 # TempMaxHeute
  outstr = outstr+getfromDict(d,["maxdailygust"],ignoreKeys)+";"                         # WindspitzeTag
  outstr = outstr+getfromDict(d,["dailyrainmm"],ignoreKeys)+";"                          # RegenHeute
  outstr += ";"                                                                          # TTemperaturdurchschnittAktuellerMonat
  outstr = outstr+getfromDict(d,["monthlyrainmm"],ignoreKeys)+";"                        # RegenAktuellerMonat
  outstr += ";"                                                                          # TSonnenzeitAktuellerMonat - WSWin: h:m - also nicht dezimal!
  outstr += ";"                                                                          # TTemperaturdurchschnittVormonat
  outstr += ";"                                                                          # TRegenVormonat
  outstr += ";"                                                                          # TSonnenzeitVormonat - WSWin: h:m - also nicht dezimal!
  outstr += ";"                                                                          # TEistage
  outstr += ";"                                                                          # TFrosttage
  outstr += ";"                                                                          # TKalteTage
  outstr += ";"                                                                          # TSommertage
  outstr += ";"                                                                          # THeißeTage
  outstr += ""                                                                           # TTropennächte(jeweils akt. Jahr)
  outstr = outstr.replace("null","")                                                     # perhaps "NULL" - clean!
  if script != "":
    outstr = modExec(nr, script, outstr)                                                 # modify outstr with external script before sending
    if outstr == execOnly: return                                                        # just run the exec-script but do not forward the string
  ret = ""
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Connection': 'Close','User-Agent': None}
      r = requests.post(url+outstr,timeout=httpTimeOut)
      ret = str(r.status_code)
      okstr = "<ERROR> " if r.status_code not in range(200,203) else ""
      if r.status_code in range(400,500): v = 400
    except requests.exceptions.Timeout as err:
      ret = "TIMEOUT"
    except requests.exceptions.ConnectionError as err:
      ret = "CONNERR"
    except requests.exceptions.RequestException as err:
      ret = "REQERR"
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  if sndlog: sndPrint(okstr + "FWD-"+nr+": " + url + outstr + " : " + ret + tries)
  return

def forwardDictToWetterCOM(url,d_in,fwd_sid,fwd_pwd,script,nr,ignoreKeys,remapKeys):
  # convert incoming metric dict to wetter.com-API
  d = remappedDict(d_in,remapKeys,nr)                          # remap keys in current dictionary
  if not "id=" in url and not "pwd=" in url: url += "?id="+str(fwd_sid)+"&pwd="+str(fwd_pwd)
  outstr = "&"
  dontuse = ("PASSKEY","PASSWORD","ID","model","freq")
  ignoreValues=["-9999","None","null"]
  isAmbientWeather = checkAmbientWeather(d)
  for key,value in d.items():
    if key in ignoreKeys or key in dontuse or (IGNORE_EMPTY and value in ignoreValues):
      None
    elif key == "PASS":
      # possibility to exchange PASS?
      None
    elif key == "dateutc":                                    # localtime YYYYMMDDHHMM
      try:
        istime = time.strftime("%Y%m%d%H%M", time.localtime(int(time.mktime(time.strptime(value.replace("%20","+").replace("%3A",":"), "%Y-%m-%d+%H:%M:%S")))))
        #time.strftime("%Y%m%d%H%M", time.localtime(int(utcToLocal(time.mktime(time.strptime(value.replace("%20","+").replace("%3A",":"), "%Y-%m-%d+%H:%M:%S"))))))
      except ValueError:
        istime = time.strftime('%Y%m%d%H%M', time.gmtime())
      outstr += "dtutc=" + str(istime) + "&"
    elif key == "tempc":
      outstr += "te=" + str(value) + "&"
    elif key == "humidity":
      outstr += "hu=" + str(value) + "&"
    elif key == "dewptc":
      outstr += "dp=" + str(value) + "&"
    elif key == "baromhpa" or key == "baromrelhpa":
      outstr += "pr=" + str(value) + "&"
    elif key == "windspeedkmh":
      outstr += "ws=" + str(round(float(value)/3.6)) + "&"
    elif key == "windgustkmh":
      outstr += "wg=" + str(round(float(value)/3.6)) + "&"
    elif key == "winddir":
      outstr += "wd=" + str(value) + "&"
    elif key == "hourlyrainmm":
      outstr += "pa=" + str(value) + "&"
    elif key == "dailyrainmm":
      outstr += "paday=" + str(value) + "&"
    elif key == "rainratemm":
      outstr += "rr=" + str(value) + "&"
    elif key == "solarradiation" or key == "solarRadiation":
      outstr += "sr=" + str(value) + "&"
    elif key == "UV" or key == "uv":
      outstr += "uv=" + str(value) + "&"
    elif key == "tempinc":
      outstr += "tei=" + str(value) + "&"
    elif key == "humidityin" or key == "indoorhumidity":
      outstr += "hui=" + str(value) + "&"
    elif "temp" in key and len(key) == 6 and key[-1] == "c":
      outstr += "teo" + str(key[4]) + "=" + str(value) + "&"
    elif "humidity" in key and len(key) == 9:
      outstr += "ho" + str(key[8]) + "=" + str(value) + "&"
    elif key == "soilmoisture1" or key == "soilhum1":
      outstr += "hus=" + str(value) + "&"
    elif key == "co2":
      outstr += "co=" + str(value) + "&"
    #elif key == "softwaretype" or key == "softwareType":
      #outstr += "sid=" + str(value) + "&"
    else:
      #if myDebug: logPrint("<DEBUG> forwardDictToWetterCOM: unknown field: " + str(key) + " with value: " + str(value))
      doNothing()
  if len(outstr) > 0 and outstr[-1] == "&": outstr = outstr[:-1]
  # add programname and version as sid (like weewx does)
  outstr += "&sid=weewx"
  if "&sid=" not in outstr: outstr += "&sid="+prgname+"&ver="+prgver
  if script != "":
    outstr = modExec(nr, script, outstr)                       # modify outstr with external script before sending
    if outstr == execOnly: return                              # just run the exec-script but do not forward the string
  ret = ""
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      r = requests.get(url+outstr,timeout=httpTimeOut)
      ret = str(r.status_code)
      okstr = "<ERROR> " if r.status_code not in range(200,203) else ""
      if r.status_code in range(400,500): v = 400
    except requests.exceptions.Timeout as err:
      ret = "TIMEOUT"
    except requests.exceptions.ConnectionError as err:
      ret = "CONNERR"
    except requests.exceptions.RequestException as err:
      ret = "REQERR"
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  if sndlog: sndPrint(okstr + "FWD-"+nr+": " + url + outstr + " : " + ret + tries)
  return

def forwardDictToWeather365(url,d_in,fwd_sid,fwd_pwd,script,nr,ignoreKeys,remapKeys):
  # convert incoming metric dict to Weather365-API acc. to https://www.weather365.net/wettersatelliten-und-wetterradar/wetter-aktuell/wetternetzwerk-mitmachen.html
  # fields et, windrun, humidex, rxsignal, txbattery are not filled yet
  # add stationid
  d = remappedDict(d_in,remapKeys,nr)                          # remap keys in current dictionary
  outstr = "stationid="+fwd_sid+"&"
  dontuse = ("PASSKEY","PASSWORD","ID","model","freq")
  ignoreValues = ["-9999","None","null"]
  isAmbientWeather = checkAmbientWeather(d)
  wdir = wdir10m = lat = lon = alt = ""
  for key,value in d.items():
    if key in ignoreKeys or key in dontuse or (IGNORE_EMPTY and value in ignoreValues):
      None
    elif key == "PASS":
      # possibility to exchange PASS?
      None
    elif key == "dateutc":                                     # datum=YYYYMMDDHHMM utctime=unixdate - perhaps better to use localtime instead
      try:
        zeit = time.localtime(int(utcToLocal(time.mktime(time.strptime(value.replace("%20","+").replace("%3A",":"), "%Y-%m-%d+%H:%M:%S")))))
      except ValueError:
        zeit = time.localtime()

      # adjust given time to 0,5,10,15,20,25,30,35,40,45,50,55
      temp = time.mktime(zeit)
      zeit = time.localtime(int(temp-(temp-(temp%3600*3600))/60%5*60))

      istime = time.strftime('%Y%m%d%H%M', zeit)
      utime  = int(time.mktime(zeit))
      utime = utime-(utime % 60)                               # remove seconds to enable pressure trend @Weather365
      outstr += "datum=" + str(istime) + "&" + "utcstamp=" + str(utime) + "&"
    elif key == "tempc":
      outstr += "t2m=" + str(value) + "&"
    elif key == "feelslikec":
      outstr += "appTemp=" + str(value) + "&"
    elif key == "dewptc":
      outstr += "dew2m=" + str(value) + "&"
    elif key == "heatindexc":
      outstr += "heat=" + str(value) + "&"
    elif key == "baromhpa" or key == "baromrelhpa":
      outstr += "press=" + str(value) + "&"
    elif key == "solarradiation" or key == "solarRadiation":
      outstr += "radi=" + str(value) + "&"
    elif key == "dailyrainmm":
      outstr += "raind=" + str(value) + "&"
    elif key == "hourlyrainmm":
      outstr += "rainh=" + str(value) + "&" + "prec_time=60" + "&"
    elif key == "rainratemm":
      outstr += "rainrate=" + str(value) + "&"
    elif key == "humidity":
      outstr += "relhum=" + str(value) + "&"
    elif "soilmoisture" in key and len(key) == 13:             # sensor1=5cm, 2=10/15cm, 3=20-30cm, 4=40-50cm
      if key[12] == "1":
        #try: value = str(200-int(value)*2)                     # convert to centibar - but is this really linear?
        #except: pass
        outstr += "soilmoisture=" + str(value) + "&"
      else:                                                    # all sensors are for different depth 5, 10, 20, 50 cm
        outstr += "soilmoisture" + str(key[12]) + "=" + str(value) + "&"
    elif key == "UV" or key == "uv":
      outstr += "uvi=" + str(value) + "&"
    elif key == "winddir":
      wdir = str(value)
    elif key == "winddir_avg10m":
      wdir10m = str(value)
    elif key == "windspeedkmh":
      outstr += "windspeed=" + str(round(float(value)/3.6)) + "&"
    elif key == "windgustkmh":
      outstr += "windgust=" + str(round(float(value)/3.6)) + "&"
    elif key == "windchillc":
      outstr += "wchill=" + str(value) + "&"
    elif key == "cloudm":
      outstr += "cloudbase=" + str(value) + "&"
    elif key == "sunhours":
      outstr += "sunh=" + str(value) + "&"
    elif "leafwetness_ch" in key and len(key) == 15:
      if key[14] == "1":
        outstr += "leafwetness=" + str(leafTo15(value)) + "&"
      else:
        outstr += "leafwetness" + str(key[14]) + "=" + str(leafTo15(value)) + "&"
    # v0.09: convert tf_chN to soiltempN
    elif "tf_ch" in key and len(key) == 7 and key[-1] == "c":  # sensor1=5cm, 2=10/15cm, 3=20-30cm, 4=40-50cm
      if key[5] == "1":
        outstr += "soiltemp=" + str(value) + "&"
      else:                                                    # all sensors are for different depth 5, 10, 20, 50 cm
        outstr += "soiltemp" + str(key[5]) + "=" + str(value) + "&"
    #elif "temp" in key and len(key) == 6 and key[-1] == "c":   # not sure what they will do with inside temps & hums
    #  outstr += "temp" + str(key[4]) + "=" + str(value) + "&"
    #elif "humidity" in key and len(key) == 9:
    #  outstr += "humidity" + str(key[8]) + "=" + str(value) + "&"
    elif key == "lat":
      lat = str(value)
    elif key == "lon":
      lon = str(value)
    elif key == "alt":
      alt = str(value)
    else:
      if myDebug: logPrint("<DEBUG> forwardDictToWeather365: unknown field: " + str(key) + " with value: " + str(value))
  # after loop

  # winddir
  if wdir10m != "":                                            # prefer 10m-average to winddir if available
    outstr += "winddir=" + wdir10m + "&"
  elif wdir != "":                                             # send only if available
    outstr += "winddir=" + wdir + "&"
  # coordinates
  if lat == "": lat = COORD_LAT                                # add coordinates if available, prefer paramter
  if lon == "": lon = COORD_LON                                # but use vars from config file if given
  if alt == "": alt = COORD_ALT                                # and send only if any data available
  if lat != "": outstr += "latitude=" + lat + "&"
  if lon != "": outstr += "longitude=" + lon + "&"
  if alt != "": outstr += "altitude=" + alt + "&"
  # clean outgoing string
  if len(outstr) > 0 and outstr[-1] == "&": outstr = outstr[:-1]
  if script != "":
    outstr = modExec(nr, script, outstr)                       # modify outstr with external script before sending
    if outstr == execOnly: return                              # just run the exec-script but do not forward the string
  ret = ""
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Connection': 'Close','User-Agent': None}
      r = requests.post(url,data=outstr,headers=headers,timeout=httpTimeOut)
      ret = str(r.status_code)
      okstr = "<ERROR> " if r.status_code not in range(200,203) else ""
      if r.status_code in range(400,500): v = 400
    except requests.exceptions.Timeout as err:
      ret = "TIMEOUT"
    except requests.exceptions.ConnectionError as err:
      ret = "CONNERR"
    except requests.exceptions.RequestException as err:
      ret = "REQERR"
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  if sndlog: sndPrint(okstr + "FWD-"+nr+": " + url + " post: " + outstr + " : " + ret + tries)
  return

def dictToREALTIME(d_in,nr,ignoreKeys,remapKeys):
  # convert the dict d_in to structured REALTIME-string
  d = remappedDict(d_in,remapKeys,nr)                          # remap keys in current dictionary
  s = ""
  now = time.localtime()
  a = []
  # create array
  for i in range(60):
    a.append("--")                                             # perhaps "NULL"
  # fill array
  a[0]  = "empty"
  a[1]  = time.strftime('%d/%m/%Y', now)
  a[2]  = time.strftime('%H:%M:%S', now)
  a[3]  = getfromDict(d,["tempc"],ignoreKeys)
  a[4]  = getfromDict(d,["humidity"],ignoreKeys)
  a[5]  = getfromDict(d,["dewptc"],ignoreKeys)
  a[6]  = getfromDict(d,["windspdkmh_avg10m"],ignoreKeys)
  a[7]  = getfromDict(d,["windspeedkmh"],ignoreKeys)
  a[8]  = getfromDict(d,["winddir"],ignoreKeys)
  a[9]  = getfromDict(d,["rainratemm"],ignoreKeys)
  a[10] = getfromDict(d,["dailyrainmm"],ignoreKeys)
  a[11] = getfromDict(d,["baromrelhpa","baromhpa"],ignoreKeys)
  a[12] = WindDirText(getfromDict(d,["winddir"],ignoreKeys),"ZZ")
  a[14] = "km/h"
  a[15] = "C"
  a[16] = "hPa"
  a[17] = "mm"
  a[19] = getfromDict(d,["pchange3"],ignoreKeys)
  a[20] = getfromDict(d,["monthlyrainmm"],ignoreKeys)
  a[21] = getfromDict(d,["yearlyrainmm"],ignoreKeys)
  a[23] = getfromDict(d,["tempinc"],ignoreKeys)
  a[24] = getfromDict(d,["humidityin","indoorhumidity"],ignoreKeys)
  a[25] = getfromDict(d,["windchillc"],ignoreKeys)
  a[27] = getfromDict(min_max,["tempc_max"],ignoreKeys)
  a[28] = time.strftime("%H:%M",time.localtime(int(getfromDict(min_max,["tempc_max_time"],ignoreKeys))))
  a[29] = getfromDict(min_max,["tempc_min"],ignoreKeys)
  a[30] = time.strftime("%H:%M",time.localtime(int(getfromDict(min_max,["tempc_min_time"],ignoreKeys))))
  a[31] = getfromDict(min_max,["windspeedkmh_max"],ignoreKeys)
  a[32] = time.strftime("%H:%M",time.localtime(int(getfromDict(min_max,["windspeedkmh_max_time"],ignoreKeys))))
  #a[33] = getfromDict(d,["maxdailygust"],ignoreKeys)
  a[33] = getfromDict(min_max,["windgustkmh_max"],ignoreKeys)
  a[34] = time.strftime("%H:%M",time.localtime(int(getfromDict(min_max,["windgustkmh_max_time"],ignoreKeys))))
  a[35] = getfromDict(min_max,["baromrelhpa_max"],ignoreKeys)
  a[36] = time.strftime("%H:%M",time.localtime(int(getfromDict(min_max,["baromrelhpa_max_time"],ignoreKeys))))
  a[37] = getfromDict(min_max,["baromrelhpa_min"],ignoreKeys)
  a[38] = time.strftime("%H:%M",time.localtime(int(getfromDict(min_max,["baromrelhpa_min_time"],ignoreKeys))))
  a[41] = getfromDict(d,["windgustkmh_max10m"],ignoreKeys)
  a[42] = getfromDict(d,["heatindexc"],ignoreKeys)
  a[44] = getfromDict(d,["uv","UV"],ignoreKeys)
  a[46] = getfromDict(d,["solarradiation"],ignoreKeys)
  a[47] = getfromDict(d,["winddir_avg10m"],ignoreKeys)
  a[48] = getfromDict(d,["hourlyrainmm"],ignoreKeys)
  a[52] = WindDirText(getfromDict(d,["winddir_avg10m"],ignoreKeys),"ZZ")
  a[53] = getfromDict(d,["cloudm"],ignoreKeys)
  a[54] = "m"
  a[56] = getfromDict(d,["sunhours"],ignoreKeys)
  a[59] = getfromDict(d,["feelslikec"],ignoreKeys)
  # create string
  for i in range(1,len(a)): s+=str(a[i])+" "
  # cut last space
  s = s[:-1]
  # clean string
  s = s.replace("null","--")                                   # perhaps "NULL"
  return s

def dictToCLIENTRAW(d_in,nr,ignoreKeys,remapKeys):
  # convert the dict d_in to structured CLIENTRAW-string
  d = remappedDict(d_in,remapKeys,nr)                                    # remap keys in current dictionary
  s = ""
  now = time.localtime()
  a = []
  # create array
  for i in range(178):
    a.append("--")                                                       # perhaps "NULL"
  # fill array
  a[0]   = "12345"
  a[1]   = kmhtokts(getfromDict(d,["windspeedkmh"],ignoreKeys),1)        # windspeed in kts
  a[2]   = kmhtokts(getfromDict(d,["windgustkmh"],ignoreKeys),1)         # windgust current in kts
  a[3]   = getfromDict(d,["winddir"],ignoreKeys)                         # winddir current
  a[4]   = getfromDict(d,["tempc"],ignoreKeys)                           # current outtemp
  a[5]   = getfromDict(d,["humidity"],ignoreKeys)                        # current outside humidity
  a[6]   = getfromDict(d,["baromrelhpa"],ignoreKeys)                     # current pressure in hPa
  a[7]   = getfromDict(d,["dailyrainmm"],ignoreKeys)
  a[8]   = getfromDict(d,["monthlyrainmm"],ignoreKeys)
  a[9]   = getfromDict(d,["yearlyrainmm"],ignoreKeys)
  a[10]  = getfromDict(d,["rainratemm"],ignoreKeys)
  a[12]  = getfromDict(d,["tempinc"],ignoreKeys)
  a[13]  = getfromDict(d,["humidityin","indoorhumidity"],ignoreKeys)
  a[20]  = getfromDict(d,["temp1c"],ignoreKeys)
  a[21]  = getfromDict(d,["temp2c"],ignoreKeys)
  a[22]  = getfromDict(d,["temp3c"],ignoreKeys)
  a[23]  = getfromDict(d,["temp4c"],ignoreKeys)
  a[24]  = getfromDict(d,["temp5c"],ignoreKeys)
  a[25]  = getfromDict(d,["temp6c"],ignoreKeys)
  a[26]  = getfromDict(d,["humidity1"],ignoreKeys)
  a[27]  = getfromDict(d,["humidity2"],ignoreKeys)
  a[28]  = getfromDict(d,["humidity3"],ignoreKeys)
  a[29]  = time.strftime('%H', now)                                      # hour
  a[30]  = time.strftime('%M', now)                                      # min
  a[31]  = time.strftime('%S', now)                                      # sec
  a[32]  = prgname+"-"+time.strftime("%H:%M:%S", now)                    # station name 2do!
  a[33]  = getfromDict(d,["lightning_num"],ignoreKeys)                   # lightning count
  a[35]  = time.strftime('%d', now)                                      # day
  a[36]  = time.strftime('%m', now)                                      # month
  a[44]  = getfromDict(d,["windchillc"],ignoreKeys)                      # windchill
  a[46]  = getfromDict(min_max,["tempc_max"],ignoreKeys)                 # daily max temp
  a[47]  = getfromDict(min_max,["tempc_min"],ignoreKeys)                 # daily min temp
  a[50]  = getfromDict(d,["pchange1"],ignoreKeys)                        # baro trend 1 hour
  a[71]  = getfromDict(d,["maxdailygust"],ignoreKeys)                    # max gust
  a[72]  = getfromDict(d,["dewptc"],ignoreKeys)                          # dew point
  try:
    a[73] = mtofeet(getfromDict(d,["cloudm"],ignoreKeys),2)              # cloud height in feet as string
  except: pass
  a[74]  = time.strftime("%d/%m/%Y", now)                                # date
  a[77]  = getfromDict(min_max,["windchillc_max"],ignoreKeys)            # daily max windchill
  a[78]  = getfromDict(min_max,["windchillc_min"],ignoreKeys)            # daily min windchill
  a[79]  = getfromDict(d,["uv","UV"],ignoreKeys)                         # UVI
  a[100] = getfromDict(d,["hourlyrainmm"],ignoreKeys)                    # rain last hour
  a[110] = getfromDict(min_max,["heatindexc_max"],ignoreKeys)            # daily max heatindex
  a[111] = getfromDict(min_max,["heatindexc_min"],ignoreKeys)            # daily min heatindex
  a[112] = getfromDict(d,["heatindexc"],ignoreKeys)                      # heat index
  a[113] = getfromDict(d,["windgustkmh_max10m"],ignoreKeys)              # wind speed avg max 2do!
  #a[114] = getfromDict(d,["lightning_num"],ignoreKeys)                   # count of last lightning - 2do
  try:
    llt = time.localtime(int(getfromDict(d,["lightning_time"],ignoreKeys)))
    a[115] = time.strftime("%H:%M:%S", llt)
    a[116] = time.strftime("%d/%m/%Y", llt)
  except ValueError:
    pass
  a[117] = getfromDict(d,["winddir_avg10m"],ignoreKeys)                  # wind dir avg
  a[118] = getfromDict(d,["lightning"],ignoreKeys)                       # distance of last lightning
  a[120] = getfromDict(d,["temp7c"],ignoreKeys)
  a[121] = getfromDict(d,["temp8c"],ignoreKeys)
  a[122] = getfromDict(d,["humidity4"],ignoreKeys)
  a[123] = getfromDict(d,["humidity5"],ignoreKeys)
  a[124] = getfromDict(d,["humidity6"],ignoreKeys)
  a[125] = getfromDict(d,["humidity7"],ignoreKeys)
  a[126] = getfromDict(d,["humidity8"],ignoreKeys)
  a[127] = getfromDict(d,["solarradiation","solarRadiation"],ignoreKeys) # solar radiation
  a[128] = getfromDict(min_max,["tempinc_max"],ignoreKeys)               # daily max intemp
  a[129] = getfromDict(min_max,["tempinc_min"],ignoreKeys)               # daily min intemp
  a[130] = getfromDict(d,["feelslikec"],ignoreKeys)                      # feelslike
  a[131] = getfromDict(min_max,["baromrelhpa_max"],ignoreKeys)           # daily max pressure
  a[132] = getfromDict(min_max,["baromrelhpa_min"],ignoreKeys)           # daily min pressure
  #a[133] = kmhtokts(getfromDict(min_max,["windgustkmh_max"],ignoreKeys),1)                                         # max gust in last hour in kts
  a[135] = time.strftime("%H:%M",time.localtime(int(getfromDict(min_max,["windgustkmh_max_time"],ignoreKeys))))    # daily max gust time hh:mm
  a[136] = getfromDict(min_max,["feelslikec_max"],ignoreKeys)            # daily max apparent temp
  a[137] = getfromDict(min_max,["feelslikec_min"],ignoreKeys)            # daily min apparent temp
  a[138] = getfromDict(min_max,["dewptc_max"],ignoreKeys)                # daily max dewpoint
  a[139] = getfromDict(min_max,["dewptc_min"],ignoreKeys)                # daily min dewpoint
  a[141] = time.strftime('%Y', now)                                      # hour
  a[157] = getfromDict(d,["soilmoisture1"],ignoreKeys)                   # just ch #1
  a[158] = kmhtokts(getfromDict(d,["windspdkmh_avg10m"],ignoreKeys),1)   # windspeed average in kts
  lat = getfromDict(d,["lat"],ignoreKeys)
  a[160] = lat if lat != "null" else COORD_LAT                           # latitude (- for southern hemispehere)
  lon = getfromDict(d,["lon"],ignoreKeys)
  a[161] = lon if lon != "null" else COORD_LON                           # longitude (- for east of GMT)
  a[163] = getfromDict(min_max,["humidity_max"],ignoreKeys)              # daily max humidity
  a[164] = getfromDict(min_max,["humidity_min"],ignoreKeys)              # daily min humidity
  a[166] = time.strftime("%H:%M",time.localtime(int(getfromDict(min_max,["windchillc_min_time"],ignoreKeys))))     # daily min windchill time hh:mm
  a[176] = getfromDict(d,["winddir_avg10m"],ignoreKeys)                  # wind dir avg (like 117?)
  a[174] = time.strftime("%H:%M",time.localtime(int(getfromDict(min_max,["tempc_max_time"],ignoreKeys))))          # daily max temp time hh:mm
  a[175] = time.strftime("%H:%M",time.localtime(int(getfromDict(min_max,["tempc_min_time"],ignoreKeys))))          # daily min temp time hh:mm
  a[177] = "!!"+prgname+prgver+"!!"                                      # wd version - end of file !!C10.37S111!! 2do!
  # create string
  for i in range(0,len(a)): s+=str(a[i])+" "
  # cut last space
  s = s[:-1]
  # clean string
  s = s.replace("null","--")                                             # perhaps "NULL"
  return s

def ddTodms(dd):                                               # genauer, aber auch sicher?
  neg = dd < 0
  deg = int(abs(float(dd)))
  mnt = (abs(dd) - deg) * 60
  sec = (mnt - int(mnt)) * 100
  return neg, int(round(deg,0)), int(mnt), int(round(sec,0))

def fillLeft(what,length,fill):
  s = str(what)
  while len(s) < length: s = fill+s
  return s

def ddTodmsAPRS(lat, lon):
  latout = lonout = ""
  try:
    neg, d, m, s = ddTodms(float(lat))
    la = fillLeft(d,2,"0")+fillLeft(m,2,"0")+"."+fillLeft(s,2,"0")
    latout = la+"S" if neg else la+"N"
    neg, d, m, s = ddTodms(float(lon))
    lo = fillLeft(d,3,"0")+fillLeft(m,2,"0")+"."+fillLeft(s,2,"0")
    lonout = lo+"W" if neg else lo+"E"
  except:
    pass
  return latout+"/"+lonout

def dictToAPRS(d,fwd_sid,ignoreKeys,remapKeys):
  ignoreValues=["-9999","None","null",""]
  #CW0003>APRS,TCPIP*: /241505 z4220.45N/07128.59W _032 /005 g008 t054 r001 p078 P048 h50 b10245 e1w
  #DUMMY>APRS,TCPIP*:/232139z5240.17N/01315.99E_088/001g001t044r002L000P010h98b10241.FOSHKplugin-0.09-GW1000A_V1.6.8
  outstr = fwd_sid+">APRS,TCPIP*:/"+time.strftime('%d%H%M', time.gmtime())+"z"+ddTodmsAPRS(COORD_LAT,COORD_LON)
  value = getfromDict(d,["winddir"],ignoreKeys)                # winddir
  if not (IGNORE_EMPTY and value in ignoreValues):
    try: outstr += "_" + fillLeft(str(round(float(value))),3,"0")
    except ValueError: outstr = "_..."
  value = getfromDict(d,["windspeedmph"],ignoreKeys)           # windspeed
  if not (IGNORE_EMPTY and value in ignoreValues):
    try: outstr += "/" + fillLeft(str(round(float(value))),3,"0")
    except ValueError: outstr = "/..."
  value = getfromDict(d,["windgustmph"],ignoreKeys)            # windgust
  if not (IGNORE_EMPTY and value in ignoreValues):
    try: outstr += "g" + fillLeft(str(round(float(value))),3,"0")
    except ValueError: outstr = "/..."
  value = getfromDict(d,["tempf"],ignoreKeys)                  # temp
  if not (IGNORE_EMPTY and value in ignoreValues): 
    try:                                                       # handle negative temperatures
      val = round(float(value))
      outstr += "t" + fillLeft(str(val),3,"0") if val > 0 else "t" + "-"+fillLeft(str(abs(val)),2,"0")
    except ValueError: outstr += "t..."
  value = getfromDict(d,["hourlyrainin","rainin"],ignoreKeys)  # rain/hour
  if not (IGNORE_EMPTY and value in ignoreValues):
    try: outstr += "r" + fillLeft(str(round(float(value)*100.0)),3,"0")
    except ValueError: pass
  #value = getfromDict(d,["24hrainin"],ignoreKeys)             # rain24
  #if not (IGNORE_EMPTY and value in ignoreValues): outstr += "p" + fillLeft(str(round(float(value)*100)),3,"0")
  value = getfromDict(d,["solarradiation"],ignoreKeys)         # solarradiation L = W/m² while sr < 999 + l for value > 1000
  if not (IGNORE_EMPTY and value in ignoreValues):
    try:
      val = round(float(value))                                # what if val >= 2000?
      if val >= 2000: raise ValueError                         # cheat 
      outstr += "L" + fillLeft(str(round(float(value))),3,"0") if val < 1000 else "l" + value[1:]
    except ValueError: pass
  value = getfromDict(d,["dailyrainin"],ignoreKeys)            # rain/day
  if not (IGNORE_EMPTY and value in ignoreValues):
    try: outstr += "P" + fillLeft(str(round(float(value)*100.0)),3,"0")
    except ValueError: pass
  value = getfromDict(d,["humidity"],ignoreKeys)               # humidity
  if not (IGNORE_EMPTY and value in ignoreValues):
    try: outstr += "h00" if value == "100" else "h" + fillLeft(str(round(float(value))),2,"0")
    except ValueError: pass
  value = getfromDict(d,["baromrelin","baromin"],ignoreKeys)   # pressure
  if not (IGNORE_EMPTY and value in ignoreValues):
    try: outstr += "b" + fillLeft(str(round(float(intohpa(float(value),2))*10)),5,"0")
    except ValueError: outstr += "b....."
  value = getfromDict(d,["stationtype","softwaretype"],ignoreKeys)   # software
  if not (IGNORE_EMPTY and value in ignoreValues):
    value = "-"+value if value not in ignoreValues else ""
    outstr += "." + prgname + "-" + prgver.replace("v","") + value
  return outstr

def forwardDictToAPRS(url,d_in,fwd_sid,fwd_pwd,script,nr,ignoreKeys,remapKeys):
  d = remappedDict(d_in,remapKeys,nr)                          # remap keys in current dictionary
  outstr = dictToAPRS(d,fwd_sid,ignoreKeys,remapKeys)
  if script != "":
    outstr = modExec(nr, script, outstr)                       # modify outstr with external script before sending
    if outstr == execOnly: return                              # just run the exec-script but do not forward the string
  # create socket
  addr = url.split(":",1)
  serverHost = addr[0]
  serverPort = 14580 if len(addr) == 1 else int(addr[1])       # default the port to 14580 if not given
  if fwd_pwd == "": fwd_pwd = "-1"                             # some stations may need a password
  ret = ""
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      sSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      sSock.connect((serverHost, serverPort))
      sSock.send(bytes('user ' + fwd_sid + ' pass ' + fwd_pwd + ' vers ' + prgname + ' ' + prgver + '\n',OutEncoding))
      sSock.send(bytes(outstr+'\n',OutEncoding))
      sSock.shutdown(0)
      sSock.close()
      ret = "OK"
      okstr = ""
    except socket.error as err:
      ret = str(err.args[0]) + " : " +err.args[1]
      okstr = "<ERROR> "
      pass
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  if sndlog: sndPrint(okstr + "FWD-"+nr+": " + url + " APRS: " + outstr + " : " + ret + tries)
  return

def postFile(url, user, password, filename, append, content):
  text = ""
  ret = ""
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      r = requests.post(url, data=({
          "user":user,
          "password":password,
          "filename":filename,
          "append":append,
          "content":content
        }), timeout=httpTimeOut)
      ret = str(r.status_code)
      okstr = "<ERROR> " if r.status_code not in range(200,203) else ""
      if r.status_code in range(400,500): v = 400
      text = r.text
    except requests.exceptions.Timeout as err:
      ret = "TIMEOUT"
    except requests.exceptions.ConnectionError as err:
      ret = "CONNERR"
    except requests.exceptions.RequestException as err:
      ret = "REQERR"
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  return(text,ret+tries)

def extractSRV(url):
  slash = url.find("/")
  if slash >= 0:
    srv = url[:slash]
    path = url[slash:]
  else:
    srv = url
    path = ""
  return(srv,path)

def ftpFile(url, user, password, filename, appendFile, content):
  ftps = False
  ret = "FAILED"
  text = ""
  # recreate url
  if "ftps://" in url:
    url = url[7:]
    ftps = True
  else:
    url = url[6:]
  srv,path = extractSRV(url)
  try:
    ftp = ftplib.FTP_TLS(srv) if ftps else ftplib.FTP(srv)
  except: pass
  #print("srv: "+srv+" path: "+path+" user: "+user+" pwd: "+password+" filename: "+filename)
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while ret != "OK" and v < httpTries:
    try:
      ftp.login(user, password)
      if ftps: ftp.prot_p()
      ftp.cwd(path)
      with io.BytesIO() as fp:
        fp.write(bytearray(content,'latin-1'))
        fp.seek(0)
        #res = ftp.storlines("STOR " + filename, fp)
        res = ftp.storlines("APPE " + filename, fp) if appendFile else ftp.storlines("STOR " + filename, fp)
        if res.startswith('226 Transfer complete'): ret = "OK"
    #except (OSError, ftplib.all_errors) as e:
    except Exception as e:
      ret = str(e)
    v += 1                                                     # count of tries
    if v < httpTries and ret != "OK": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  return (text,ret+tries)

def forwardDictToFile(url,d_in,fwd_sid,fwd_pwd,status,script,nr,ignoreKeys,remapKeys,fwd_type):
  # convert the given dict and export this file to url-dependend target (use default filename if not given in url)
  # used by REALTIMETXT, CLIENTRAWTXT, CSVFILE, TXTFILE, TEXTFILE, RAWTEXT, WSWIN
  ret = ""
  appendFile = False
  if url[-1] == "/":                                           # path given but no name so use defaults
    path = url
    filename = ""
  else:                                                        # there is a file name - so split path and filename
    path, filename = os.path.split(url)
    path += "/"
  # create outstr from dict
  if fwd_type == "REALTIMETXT":                                # realtime.txt
    if filename == "" or "http://" in url or "https://" in url: filename = "realtime.txt"
    outstr = dictToREALTIME(d_in,nr,ignoreKeys,remapKeys)
  elif fwd_type == "CLIENTRAWTXT":                             # clientraw.txt
    if filename == "" or "http://" in url or "https://" in url: filename = "clientraw.txt"
    outstr = dictToCLIENTRAW(d_in,nr,ignoreKeys,remapKeys)
  elif fwd_type == "CSVFILE":                                  # CSV file
    if filename == "" or "http://" in url or "https://" in url: filename = "FOSHKplugin.csv"
    outstr = dictToString(d_in,";",True,ignoreKeys,{},True,True,False)     # just a textfile; separated with ";"
  elif fwd_type == "WSWIN":                                    # WSWin export
    if filename == "" or "http://" in url or "https://" in url: filename = "wswin.csv"
    appendFile = True
    outstr = dictToWSWin(d_in,nr,ignoreKeys,remapKeys)
  else:                                                        # TXTFILE = metric, RAWTEXT = imperial, unknown
    if filename == "" or "http://" in url or "https://" in url: 
      filename = "rawtext.txt" if fwd_type == "RAWTEXT" else "FOSHKplugin.txt"
    # 2do: need to append some keys
    outstr = dictToString(d_in,"\n",False,ignoreKeys,[],True,True,False)   # just a textfile; separated with "\n"
  if script != "":
    outstr = modExec(nr, script, outstr)                       # modify outstr with external script before sending
    if outstr == execOnly: return                              # just run the exec-script but do not forward the string
  if "http://" in url or "https://" in url:                    # send via http/POST
    typ = "post"
    text,ret = postFile(url, fwd_sid, fwd_pwd, filename, appendFile, outstr)
  elif "ftp://" in url or "ftps://" in url:                    # save to FTP(S) server
    typ = "ftp"
    text,ret = ftpFile(path, fwd_sid, fwd_pwd, filename, appendFile, outstr)
  else:                                                        # save as local file
    typ = "save"
    try:
      if appendFile:
        if not os.path.exists(path+filename):
          with open(path+filename, 'w') as write_file: write_file.write(WSWinCSVHeader)
        with open(path+filename, 'a+') as write_file: write_file.write(outstr)
      else:
        with open(path+filename, 'w') as write_file: write_file.write(outstr)
      ret = "OK"
    except:
      ret = "ERROR"
      pass
  okstr = "<ERROR> " if ret[:2] != "OK" and ret[:3] != "200" else ""
  if sndlog: sndPrint(okstr + "FWD-"+nr+": " + typ + " " + path + filename + " : " + ret)
  return

def dictToWSWin(d_in,nr,ignoreKeys,remapKeys):
  # 2do: implement remapKeys
  outstr = ""
  d = remappedDict(d_in,remapKeys,nr)                          # remap keys in current dictionary
  #d = d_in.copy()
  d.update(min_max)                                            # append min_max values
  # remap keys here
  now = time.localtime()                                       # 2do: better to use original date/time
  outstr += time.strftime('%d.%m.%Y', now) + ";"                                                             # date (TT.MM.JJJJ)
  outstr += time.strftime('%H:%M', now) + ";"                                                                # time (hh:mm)
  outstr += getfromDict(d,["tempinc"],ignoreKeys).replace(".",",") + ";"                                     # 1     idTempInnen
  outstr += getfromDict(d,["humidityin","indoorhumidity"],ignoreKeys).replace(".",",") + ";"                 # 17    idFeuchteInnen
  outstr += getfromDict(d,["baromrelhpa"],ignoreKeys).replace(".",",") + ";"                                 # 133   idLuftdruck
  outstr += getfromDict(d,["tempc"],ignoreKeys).replace(".",",") + ";"                                       # 2     idTemp1
  outstr += getfromDict(d,["humidity"],ignoreKeys).replace(".",",") + ";"                                    # 18    idFeuchte1
  outstr += getfromDict(d,["windspeedkmh"],ignoreKeys).replace(".",",") + ";"                                # 35    idWindgeschw
  outstr += getfromDict(d,["winddir"],ignoreKeys).replace(".",",") + ";"                                     # 36    idWindrichtung
  outstr += getfromDict(d,["windgustkmh"],ignoreKeys).replace(".",",") + ";"                                 # 45    idWindböen
  outstr += getfromDict(d,["dailyrainmm"],ignoreKeys).replace(".",",") + ";"                                 # 134   idRegen24
  outstr += getfromDict(d,["solarradiation","solarRadiation"],ignoreKeys).replace(".",",") + ";"             # 42    idSolar
  outstr += getfromDict(d,["uv","UV"],ignoreKeys).replace(".",",") + ";"                                     # 41    idUV
  outstr += getfromDict(d,["temp1c"],ignoreKeys).replace(".",",") + ";"                                      # 3     idTemp2
  outstr += getfromDict(d,["humidity1"],ignoreKeys).replace(".",",") + ";"                                   # 19    idFeuchte2
  outstr += getfromDict(d,["temp2c"],ignoreKeys).replace(".",",") + ";"                                      # 4     idTemp3
  outstr += getfromDict(d,["humidity2"],ignoreKeys).replace(".",",") + ";"                                   # 20    idFeuchte3
  outstr += getfromDict(d,["temp3c"],ignoreKeys).replace(".",",") + ";"                                      # 5     idTemp4
  outstr += getfromDict(d,["humidity3"],ignoreKeys).replace(".",",") + ";"                                   # 21    idFeuchte4
  outstr += getfromDict(d,["temp4c"],ignoreKeys).replace(".",",") + ";"                                      # 6     idTemp5
  outstr += getfromDict(d,["humidity4"],ignoreKeys).replace(".",",") + ";"                                   # 22    idFeuchte5
  outstr += getfromDict(d,["temp5c"],ignoreKeys).replace(".",",") + ";"                                      # 7     idTemp6
  outstr += getfromDict(d,["humidity5"],ignoreKeys).replace(".",",") + ";"                                   # 23    idFeuchte6
  outstr += getfromDict(d,["temp6c"],ignoreKeys).replace(".",",") + ";"                                      # 8     idTemp7
  outstr += getfromDict(d,["humidity6"],ignoreKeys).replace(".",",") + ";"                                   # 24    idFeuchte7
  outstr += getfromDict(d,["soilmoisture1","soilmoisture"],ignoreKeys).replace(".",",") + ";"                # 29    idMoisture1
  outstr += getfromDict(d,["soilmoisture2"],ignoreKeys).replace(".",",") + ";"                               # 30    idMoisture2
  outstr += getfromDict(d,["soilmoisture3"],ignoreKeys).replace(".",",") + ";"                               # 31    idMoisture3
  outstr += getfromDict(d,["soilmoisture4"],ignoreKeys).replace(".",",") + ";"                               # 32    idMoisture4
  outstr += leafTo15(getfromDict(d,["leafwetness_ch1","leafwetness"],ignoreKeys).replace(".",",")) + ";"     # 25    idLeafWet1
  outstr += leafTo15(getfromDict(d,["leafwetness_ch2","leafwetness2"],ignoreKeys).replace(".",",")) + ";"    # 26    idLeafWet2
  outstr += leafTo15(getfromDict(d,["leafwetness_ch3","leafwetness3"],ignoreKeys).replace(".",",")) + ";"    # 27    idLeafWet3
  outstr += leafTo15(getfromDict(d,["leafwetness_ch4","leafwetness4"],ignoreKeys).replace(".",",")) + ";"    # 28    idLeafWet4
  outstr += str(getfromDict(d,["sunmins"],ignoreKeys)).replace(".",",") + ";"                                # 37    idSonnenZeit in minutes
  outstr += getfromDict(d,["tf_ch1c"],ignoreKeys).replace(".",",") + ";"                                     # 13    idTempSoil1 from WN34#1
  outstr += getfromDict(d,["tf_ch2c"],ignoreKeys).replace(".",",") + ";"                                     # 14    idTempSoil2 from WN34#2
  outstr += getfromDict(d,["tf_ch3c"],ignoreKeys).replace(".",",") + ";"                                     # 15    idTempSoil3 from WN34#3
  outstr += getfromDict(d,["tf_ch4c"],ignoreKeys).replace(".",",") + ";"                                     # 16    idTempSoil4 from WN34#4
  outstr += getfromDict(d,["model"],ignoreKeys) + ";"                                                        # model
  outstr += getfromDict(d,["stationtype"],ignoreKeys) + ";"                                                  # stationtype
  if len(outstr) > 0 and outstr[-1] == ";":
    outstr = outstr[:-1]                                       # delete last semicolon
  outstr += "\r\n"                                             # line end
  outstr = outstr.replace("null","")                           # perhaps "NULL"
  return outstr

def forwardDictToMQTT(url,d_in,fwd_sid,fwd_pwd,status,script,nr,ignoreKeys,remapKeys,MQTTsendMin,metric):
  # 2do: convert minmax to imperial, add missing elements from metric dict
  # used by MQTTMET and MQTTIMP
  global last_mqtt
  global MQTTsendTime
  MQTTsendAll = False
  if MQTTsendMin > 0:                                          # only transfer changed values but every given minutes the complete set
    MQTTonChangeOnly = True                                    # send only changed values via MQTT - set to False for any value every time
  else: MQTTonChangeOnly = False                               # send all data every time
  d = remappedDict(d_in,remapKeys,nr)                          # remap keys in current dictionary
  #d = d_in.copy()
  prefix = level = ""
  port = 1883                                                  # default MQTT port
  ignoreValues=["-9999","None","null"]                         # ignoreKeys = blacklist keys; ignoreValues = empty values
  d_out = []                                                   # empty output array to be filled
  d_out_len = 0
  i = url.find("%")
  if i > 0:
    prefix = url[i+1:]
    url = url[:i]
  i = url.find("@")                                            # perl does not accept a backslash with Config::Simple - so use @ instead
  if i > 0:
    level = url[i+1:]
    url = url[:i]
  else: level = SID
  i = url.find(":")
  if i > 0:
    port = url[i+1:]
    try:
      port = int(port)
    except ValueError:
      port = 1883
    url = url[:i]
  srv = url
  if metric: d.update(min_max)                                 # append min_max values
  else: d.update(metricToImpDict(min_max,[],ignoreValues))     # append min_max values (convert from imperial)
  if status: d.update(addStatusToDict(d, True))                # append status to the dict d if set
  # check if complete send is necessary
  if time.time() >= MQTTsendTime + (MQTTsendMin * 60): MQTTsendAll = True
  # create output list
  for key,value in d.items():
    if key in ignoreKeys or (IGNORE_EMPTY and value in ignoreValues):
      None
    elif not MQTTonChangeOnly or MQTTsendAll or (MQTTonChangeOnly and (key not in last_mqtt or value != last_mqtt[key])):
      d_out.append({'topic':level + "/"+prefix+key,'payload': strToNum(value)})
      #if myDebug: logPrint("<DEBUG> "+level+"/"+prefix+key+": "+str(strToNum(value)))
  # modify the list before sending with external script (list->str->script->list)
  try:  
    if script != "":
      d_out = json.loads(modExec(nr, script, json.dumps(d_out))) # modify outstr with external script before sending
      if json.dumps(d_out) == execOnly: return                   # just run the exec-script but do not forward the string
  except: pass

  # send to MQTT server
  ret = ""
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      d_out_len = len(d_out)
      if d_out_len > 0:
        publish.multiple(d_out, hostname=srv, port=port, auth={'username':fwd_sid, 'password':fwd_pwd})
        last_mqtt = d.copy()
        if MQTTsendAll: MQTTsendTime = int(time.time())        # save time of complete MQTT send
      ret = "OK"
      okstr = ""
    except Exception as err:
      ret = str(err)
      pass
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  if sndlog: sndPrint(okstr + "FWD-"+nr+": MQTT sending of " + str(d_out_len) + " topics with level " + level + "/" + prefix + " to " + srv + ":" + str(port) + ": " + ret + tries)
  return

def quoteString(s):
  noquote = ("True","False","None")
  if type(s) != str: s = str(s)
  if s in noquote: None
  elif s == "null" or s == "": s = "None"
  else:
    try:
      s = str(int(s)) if not "." in s else str(float(s))
    except:
      s = "\"" + s.replace(",","\,") + "\""
  return s

def forwardDictToInfluxDB(url,d_in,fwd_sid,fwd_pwd,status,script,nr,ignoreKeys,remapKeys,metric):
  # used by INFLUXMET and INFLUXIMP
  # initialize vars
  d = remappedDict(d_in,remapKeys,nr)                          # remap keys in current dictionary
  #d = d_in.copy()
  port = 8086                                                  # default InfluxDB port
  ignoreValues=["-9999","None","null","",None]                 # ignoreKeys = blacklist keys; ignoreValues = empty values

  # prepare string for InfluxDB line protocol
  tagarr = ("PASSKEY","ID","model","stationtype")
  iflstr = "measurement,"
  for i in range(len(tagarr)):
    tmp = getfromDict(d,[tagarr[i]])
    if tagarr[i] not in ignoreKeys and tmp not in ignoreValues: iflstr += tagarr[i] + "=" + tmp + ","
  msystem = "metric" if metric else "imperial"
  iflstr += "Forward" + "=" + nr + "," + "SID" + "=" + defSID + "," + "msystem" + "=" + msystem + " " # end of tag field

  for key, value in d.items():
    if key not in ignoreKeys and value not in ignoreValues: iflstr += key + "=" + quoteString(value) + ","

  # add missing keys from metric dict
  if not metric:
    missing = ["loxtime", "lightning_loxtime", "ptrend1", "wnowlvl", "wnowtxt", "ptrend3", "wproglvl", "wprogtxt"]
    for key in missing:
      value = getfromDict(last_d_m,[key])
      if key not in ignoreKeys and value not in ignoreValues: iflstr += key + "=" + quoteString(value) + ","
    missing = ["pchange1", "pchange3"]
    for key in missing:
      value = getfromDict(last_d_m,[key])
      try:
        value = str(hpatoin(float(value),4))
      except:
        value = "null"
      if key not in ignoreKeys and value not in ignoreValues: iflstr += key + "=" + quoteString(value) + ","

  mm = metricToImpDict(min_max,[],ignoreValues) if not metric else min_max
  for key, value in mm.items():
    if key not in ignoreKeys and value not in ignoreValues: iflstr += key + "=" + quoteString(value) + ","

  if status: iflstr += getStatusString(",",True)               # append status to line if status set

  if len(iflstr) > 0 and iflstr[-1] == ",": iflstr = iflstr[:-1]
  #print(iflstr)

  # parse url
  i = url.find("://")
  if i > 0:
    ssl = True if i == 5 else False                            # SSL or unsecured
    url = url[i+3:]
  i = url.find("@")                                            # find database name dbname
  if i > 0:
    dbname = url[i+1:]
    url = url[:i]
  else: dbname = SID
  i = url.find(":")
  if i > 0:
    port = url[i+1:]
    try:
      port = int(port)                                         # port to send data to
    except ValueError:
      port = 8086
    url = url[:i]
  srv = url

  # modify the list before sending with external script (list->str->script->list)
  try:
    if script != "":
      iflstr = modExec(nr, script, iflstr)                       # modify outstr with external script before sending
      if iflstr == execOnly: return                              # just run the exec-script but do not forward the string
  except: pass

  # everything should now be clear: server, port, database, user, password and data to send (iflstr) - transfer now
  if myDebug: sndPrint("<DEBUG> toINFLUXDB: srv: "+srv+" port: "+str(port)+" db: "+dbname+" usr: "+fwd_sid+" pwd: "+fwd_pwd+" ssl: "+str(ssl)+" url: "+url+" *"+iflstr+"*")

  ret = ""
  okstr = "<ERROR> "
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      d_out_len = iflstr.count(",")
      client = InfluxDBClient(host=srv, port=port, username=fwd_sid, password=fwd_pwd, ssl=ssl, verify_ssl=False)
      client.create_database(dbname)                           # generate the database prophylactically
      client.switch_database(dbname)                           # connect to database
      if client.write_points(iflstr, protocol='line'):         # store data to database
        ret = "OK"
        okstr = ""
    except Exception as err:
      ret = str(err)
      if len(ret) >= 3 and ret[:3] == "400": v = 400           # don't try again on local error
      pass
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  if sndlog: sndPrint(okstr + "FWD-"+nr+": InfluxDB sending of " + str(d_out_len) + " values to " + dbname + "@" + srv + ":" + str(port) + ": " + ret + tries)
  return

def getTimeZone():
  s = os.environ.get('TZ')
  if not s and os.path.exists('/etc/timezone'): s = open('/etc/timezone').read()
  if not s: s = ""
  return s.strip()

def WetterPrognose(diff,lang):
  arr=[
        ["Sturm mit Hagel","Regen/Unwetter","regnerisch","baldiger Regen","gleichbleibend","lange schön","schön & labil","Sturmwarnung"],
        ["Storm met hagel","Regen/storm","regenachtig","binnenkort regen","constante","lang mooi","mooi en onstabiel","Storm waarschuwing"],
        ["Tempête de grêle","Pluie / tempête","pluvieux","bientôt la pluie","constant","longtemps belle","beau et instable","Avertissement de tempête"],
        ["Tormenta con granizo","Tormenta de lluvia","lluvioso","pronto lloverá","constante","continuo hermosa","hermosa e inestable","Aviso de tormenta"],
        ["Búrka s krupobitím","Dážď/búrka","daždivý","skoro dážď","konštantný","dlho krásne","krásne a nestabilné","Varovanie pred búrkou"],
        ["storm with hail","rain/storm","rainy","soon rain","constant","nice for a long time","nice & unstable","storm warning"]
      ]
  if lang == "DE": zeile = 0
  elif lang == "NL": zeile = 1
  elif lang == "FR": zeile = 2
  elif lang == "ES": zeile = 3
  elif lang == "SK": zeile = 4
  else: zeile = 5                                              # defaults to english
  if diff <= -8:                    wproglvl = 0               # Sturm mit Hagel
  elif diff <= -5 and diff > -8:    wproglvl = 1               # Regen/Unwetter
  elif diff <= -3 and diff > -5:    wproglvl = 2               # regnerisch
  elif diff <= -0.5 and diff > -3:  wproglvl = 3               # baldiger Regen
  elif diff <= 0.5 and diff > -0.5: wproglvl = 4               # gleichbleibend
  elif diff <= 3 and diff > 0.5:    wproglvl = 5               # lange schön
  elif diff <= 5 and diff > 3:      wproglvl = 6               # schön & labil
  elif diff > 5:                    wproglvl = 7               # Sturmwarnung
  wprogtxt = arr[zeile][wproglvl]
  return (wproglvl,wprogtxt)

def WetterNow(hpa,lang):
  arr=[
        ["stürmisch, Regen", "regnerisch", "wechselhaft", "sonnig", "trocken, Gewitter"],
        ["stormachtig, regen","regenachtig","veranderlijk","zonnig","droog, onweer"],
        ["orageux, pluie", "pluvieux", "changeable", "ensoleillé", "sec, orage"],
        ["tormentoso, lluvia", "lluvioso", "cambiable", "soleado", "seco, tormenta"],
        ["búrky, dážď", "daždivý","premenlivý","slnečno","suchá, búrka"],
        ["stormy, rainy", "rainy", "unstable", "sunny", "dry, thunderstorm"]
      ]
  if lang == "DE": zeile = 0
  elif lang == "NL": zeile = 1
  elif lang == "FR": zeile = 2
  elif lang == "ES": zeile = 3
  elif lang == "SK": zeile = 4
  else: zeile = 5                                              # defaults to english
  if hpa <= 980:                    wnowlvl = 0                # stürmisch, Regen
  elif hpa > 980 and hpa <= 1000:   wnowlvl = 1                # regnerisch
  elif hpa > 1000 and hpa <= 1020:  wnowlvl = 2                # wechselhaft
  elif hpa > 1020 and hpa <= 1040:  wnowlvl = 3                # sonnig
  elif hpa > 1040:                  wnowlvl = 4                # trocken, Gewitter
  wnowtxt = arr[zeile][wnowlvl]
  return (wnowlvl,wnowtxt)

def WindDirText(wdir,lang):
  arr=[
        ["N","NNE","NE","ENE","E","ESE", "SE", "SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"],
        ["Nord","Nordnordost","Nordost","Ostnordost","Ost","Ostsüdost","Südost","Südsüdost","Süd","Südsüdwest","Südwest","Westsüdwest","West","Westnordwest","Nordwest","Nordnordwest"],
        ["noord","noordnoordoost","noordoost","oostnoordoost","oost","oostzuidoost","zuidoost","zuidzuidoost","zuid","zuidzuidwest","zuidwest","westzuidwest","west","westnoordwest","noordwest","noordnoordwest"],
        ["Nord","Nord-nord-est","Nord-est","Est Nord-Est","Est","Est-sud-est","Sud-est","Sud-sud-est","Sud","Sud-sud-ouest","sud-ouest","Ouest sud-ouest","Ouest","Ouest nord-ouest","Nord Ouest","Nord nord-ouest"],
        ["norte","norte-noreste","noreste","este-noreste","este","este-sureste", "sureste", "sur-sureste","sur","sur-suroeste","suroeste","oeste-suroeste","oeste","oeste-noroeste","noroeste","norte-noroeste"],
        ["sever","Severo-severovýchod","severovýchod","Na východ severovýchod","východ","juhovýchodne","juhovýchodnej","Juho-juhovýchodne","juh","Juho-juhozápadne","juhozápadnej","Západne juhozápadne","západ","Západne severozápadne","severozápad","Severozápadne"],
        ["north","north-northeast","northeast","east-northeast","east","east-southeast","southeast","south-southeast","south","south-southwest","southwest","west-southwest","west","west-northwest","north-west","north-northwest"],
        ["N","N-NO","NO","O-NO","O","O-SO", "SO", "S-SO","S","S-SW","SW","W-SW","W","W-NW","NW","N-NW"]
      ]
  if lang == "ZZ": zeile = 0
  elif lang == "DE": zeile = 1
  elif lang == "NL": zeile = 2
  elif lang == "FR": zeile = 3
  elif lang == "ES": zeile = 4
  elif lang == "SK": zeile = 5
  elif lang == "XX": zeile = 7
  else: zeile = 6
  try:
    val=int((float(wdir)/22.5)+.5)
    s = arr[zeile][(val % 16)]
  except ValueError: s = "null"
  return s

def DictToW4L(d, sep, metric):
  global feldanzahl
  global myLanguage
  s = ""
  try:
    a = []
    for i in range(0,w4l_feldanzahl):
      a.append("")
    # W4L erwartet fuer 0 und 1 localtime statt UTC
    dateutc = getfromDict(d,["dateutc"])
    utime = time.mktime(time.strptime(dateutc, "%Y-%m-%d+%H:%M:%S"))
    offset = (-1*time.timezone)
    if time.localtime(utime)[8]: offset = offset + 3600
    utime = utime + offset
    # 0 Unixtime
    a[0]  = str(int(time.mktime(time.strptime(dateutc, "%Y-%m-%d+%H:%M:%S"))+offset))
    # 1 TimeString
    a[1]  = str(time.strftime('%a, %d %b %Y %H:%M:%S %z', time.localtime(utime)))
    # 2 Zeitzone
    a[2]  = time.tzname[0]
    # 3 Zeitzone Name/Ort
    a[3]  = getTimeZone()
    # 4 Zeitzone Offset
    a[4]  = str(time.strftime('%z', time.localtime(utime)))
    a[5]  = getfromDict(d,["neighborhood"]).replace("%20"," ")
    a[7]  = getfromDict(d,["country"]).replace("%20"," ")
    a[8]  = getfromDict(d,["lat"])
    if a[8] == "null" and COORD_LAT != "": a[8] = COORD_LAT
    a[9]  = getfromDict(d,["lon"])
    if a[9] == "null" and COORD_LON != "": a[9] = COORD_LON
    a[10] = getfromDict(d,["alt"])
    if a[10] == "null" and COORD_ALT != "": a[10] = COORD_ALT
    a[11] = getfromDict(d,["temp","tempc","tempf"])
    a[12] = getfromDict(d,["feelslikec","feelslikef"])
    a[13] = getfromDict(d,["humidity","humidityin","indoorhumidity"])
    a[14] = WindDirText(getfromDict(d,["winddir"]),myLanguage)
    a[15] = getfromDict(d,["winddir"])
    a[16] = getfromDict(d,["windspeedkmh","windspeedmph","windSpeed"])
    a[17] = getfromDict(d,["windgustkmh","windgustmph","windGust"])
    a[18] = getfromDict(d,["windchillc","windchillf","windChill"])
    a[19] = getfromDict(d,["baromrelhpa","baromhpa","pressure","baromrelin","baromin"])
    a[20] = getfromDict(d,["dewptc","dewptf","dewpt"])
    a[22] = getfromDict(d,["solarradiation","solarRadiation"])
    a[23] = getfromDict(d,["heatindexc","heatindexf"])
    a[24] = getfromDict(d,["uv","UV"])
    a[25] = getfromDict(d,["precipTotal","dailyrainin","dailyrainmm"])
    a[26] = getfromDict(d,["precipRate","hourlyrainin","hourlyrainmm","rainmm"])
  except: pass
  for i in range(0,len(a)):
    if a[i] != "null": s += str(a[i])
    if i < len(a)-1: s += "|"
  return s

# ab v0.06 aktiv
def getDewPointF(temp, hum):        # in/out: °F
  try:
    temp = round((float(temp)-32)*5/9.0,1)
    s1 = math.log(float(hum) / 100.0)
    s2 = (float(temp) * 17.625) / (float(temp) + 243.04)
    s3 = (17.625 - s1) - s2
    dp = 243.04 * (s1 + s2) / s3           # in °C
    dp = round((float(dp) * 9/5)+32,1)     # in °F
  except ValueError:
    dp = -9999
  return dp

def getWindChillF(temp, wspeed):
  #return 35.74+0.6215*temp + (0.4275*temp - 35.75) * (wspeed ** 0.16) if wspeed > 0 else temp
  return 35.74 + (0.6215*temp) - 35.75*(wspeed**0.16) + ((0.4275*temp)*(wspeed**0.16)) if temp <= 50 and wspeed >= 3 else temp

def getHeatIndex(temp, hum):
  HI = 0.5 * (temp + 61. + (temp - 68.) * 1.2 + hum * 0.094)
  if HI >= 80:
    HI = -42.379 + (2.04901523 * temp) + (10.14333127 * hum) + (-0.22475541 * temp * hum) + (-6.83783e-3*temp**2) + (-5.481717e-2*hum**2) + (1.22874e-3*temp**2 * hum) + (8.5282e-4*temp*hum**2) + (-1.99e-6*temp**2*hum**2)
  return HI

def PM25toAQI(C):
  if (type(C) != float):
    return(-9999)
  elif C < 12.1:
    I_high =  50
    I_low  =   0
    C_high =  12
    C_low  =   0
  elif C < 35.5:
    I_high = 100
    I_low  =  51
    C_high = 35.4
    C_low  = 12.1
  elif C < 55.5:
    I_high = 150
    I_low  = 101
    C_high = 55.4
    C_low  = 35.5
  elif C < 150.5:
    I_high = 200
    I_low  = 151
    C_high = 150.4
    C_low  = 55.5
  elif C < 250.5:
    I_high = 300
    I_low  = 201
    C_high = 250.4
    C_low  = 150.5
  elif C < 350.5:
    I_high = 400
    I_low  = 301
    C_high = 350.4
    C_low  = 250.5
  else:                          # changed with v0.07: previously only values below 500.5 were considered
    I_high = 500
    I_low  = 401
    C_high = 500.4
    C_low  = 350.5
  I = int(round((I_high - I_low) / (C_high - C_low) * (C - C_low) + I_low))
  return(I)

def PM10toAQI(C):
  if (type(C) != float):
    return(-9999)
  elif C < 55:
    I_high =  50
    I_low  =   0
    C_high =  54
    C_low  =   0
  elif C < 155:
    I_high = 100
    I_low  =  51
    C_high = 154
    C_low  = 55
  elif C < 255:
    I_high = 150
    I_low  = 101
    C_high = 254
    C_low  = 155
  elif C < 355:
    I_high = 200
    I_low  = 151
    C_high = 354
    C_low  = 255
  elif C < 425:
    I_high = 300
    I_low  = 201
    C_high = 424
    C_low  = 355
  elif C < 505:
    I_high = 400
    I_low  = 301
    C_high = 504
    C_low  = 425
  else:                          # changed with v0.07: previously only values below 605 were considered
    I_high = 500
    I_low  = 401
    C_high = 604
    C_low  = 505
  I = int(round((I_high - I_low) / (C_high - C_low) * (C - C_low) + I_low))
  return(I)

def AQIlevel(AQI):               # US AQI
  level = 0
  try:
    if AQI <= 50: level = 1      # 0 to 50     Good                            Green
    elif AQI <= 100: level = 2   # 51 to 100   Moderate                        Yellow
    elif AQI <= 150: level = 3   # 101 to 150  Unhealthy for Sensitive Groups  Orange
    elif AQI <= 200: level = 4   # 151 to 200  Unhealthy                       Red
    elif AQI <= 300: level = 5   # 201 to 300  Very Unhealthy                  Purple
    else: level = 6              # 301 to 500  Hazardous                       Maroon
  except ValueError: pass
  return level

def CO2level(co2):               # according to https://www.breeze-technologies.de/de/blog/calculating-an-actionable-indoor-air-quality-index/ and https://sensebox.de/docs/CO2-Ampel_Lehrhandreichung.pdf
  level = 0
  try:
    if co2 <= 400: level = 1     # 0 to 400     Excellent                      Green
    elif co2 <= 1000: level = 2  # 400 to 1000  Fine, unbedenklich             Green
    elif co2 <= 1500: level = 3  # 1000 to 1500 Moderate, Lueften              Yellow
    elif co2 <= 2000: level = 4  # 1500 to 2000 Poor, Lueften!                 Red
    elif co2 <= 5000: level = 5  # 2000 to 5000 Very Poor, inakzeptabel        Purple
    else: level = 6              # from 5000    Severe                         Maroon
  except ValueError: pass
  return level

def getFeelsLikeF(temp, hum, wspeed):
  if temp <= 50 and wspeed > 3:
    FEELS_LIKE = getWindChillF(temp, wspeed)
  elif temp >= 80:
    FEELS_LIKE = getHeatIndex(temp, hum)
  else:
    FEELS_LIKE = temp
  return FEELS_LIKE

def addDataToLine(line, what, newvalue, overwrite):
  # sucht what in line und ersetzt mit newvalue oder haengt an line an
  d = stringToDict(line,"&")
  outstr = ""
  newline = ""
  global min_max
  if not what in d.keys():
    # gibt es noch nicht
    if what == "windchillf":
      try:
        temp = float(getfromDict(d,["tempf"]))
        wspeed = float(getfromDict(d,["windSpeed","windspeedmph"]))
        newvalue = round(getWindChillF(temp, wspeed),1)
        outstr += "&" + what + "=" + str(newvalue)
      except (ValueError, KeyError): pass
    elif what == "dewptf":
      try:
        temp = float(getfromDict(d,["tempf"]))
        hum = float(getfromDict(d,["humidity"]))
        newvalue = getDewPointF(temp, hum)
        #if myDebug: logPrint("<DEBUG> temp: " + str(temp) + "°F (" + str(ftoc(temp,1)) + "°C) hum: " + str(hum) + " dp: " + str(newvalue) + "°F (" + str(ftoc(newvalue,1)) + "°C)")
        outstr += "&" + what + "=" + str(newvalue)
      except (ValueError, KeyError): pass
    elif what == "feelslikef":
      try:
        temp = float(getfromDict(d,["tempf"]))
        hum = float(getfromDict(d,["humidity"]))
        wspeed = float(getfromDict(d,["windSpeed","windspeedmph"]))
        newvalue = round(getFeelsLikeF(temp, hum, wspeed),1)
        outstr += "&" + what + "=" + str(newvalue)
      except (ValueError, KeyError): pass
    elif what == "heatindexf":
      try:
        temp = float(getfromDict(d,["tempf"]))
        hum = float(getfromDict(d,["humidity"]))
        newvalue = round(getHeatIndex(temp, hum),1)
        outstr += "&" + what + "=" + str(newvalue)
      except (ValueError, KeyError): pass
    elif what == "pm25_AQI":
      for i in range(1,5):
        try:
          i_s = str(i)
          pm25 = float(getfromDict(d,["pm25_ch"+i_s]))
          AQI = PM25toAQI(pm25)
          outstr += "&" + "pm25_AQI_ch" + i_s + "=" + str(AQI)
          # AQI-level 1..6
          outstr += "&" + "pm25_AQIlvl_ch" + i_s + "=" + str(AQIlevel(AQI))
          pm25 = float(getfromDict(d,["pm25_avg_24h_ch"+i_s]))
          AQI = PM25toAQI(pm25)
          outstr += "&" + "pm25_AQI_avg_24h_ch" + i_s + "=" + str(AQI)
          # AQI-level 24h 1..6
          outstr += "&" + "pm25_AQIlvl_avg_24h_ch" + i_s + "=" + str(AQIlevel(AQI))
          # same for PM10
          pm10 = float(getfromDict(d,["pm10_ch"+i_s]))
          AQI = PM10toAQI(pm10)
          outstr += "&" + "pm10_AQI_ch" + i_s + "=" + str(AQI)
          # AQI-level 1..6
          outstr += "&" + "pm10_AQIlvl_ch" + i_s + "=" + str(AQIlevel(AQI))
          pm10 = float(getfromDict(d,["pm10_avg_24h_ch"+i_s]))
          AQI = PM10toAQI(pm10)
          outstr += "&" + "pm10_AQI_avg_24h_ch" + i_s + "=" + str(AQI)
          # AQI-level 24h 1..6
          outstr += "&" + "pm10_AQIlvl_avg_24h_ch" + i_s + "=" + str(AQIlevel(AQI))
        except (ValueError, KeyError): pass
      # v0.07: for WH45 CO2 & AQI-calculation
      try:
        co2 = float(getfromDict(d,["co2"]))
        # CO2-level 1..6
        outstr += "&" + "co2lvl" + "=" + str(CO2level(co2))
        pm25 = float(getfromDict(d,["pm25_co2"]))
        AQI = PM25toAQI(pm25)
        outstr += "&" + "pm25_AQI_co2" + "=" + str(AQI)
        # AQI-level 1..6
        outstr += "&" + "pm25_AQIlvl_co2" + "=" + str(AQIlevel(AQI))
        pm25 = float(getfromDict(d,["pm25_24h_co2"]))
        AQI = PM25toAQI(pm25)
        outstr += "&" + "pm25_AQI_24h_co2" + "=" + str(AQI)
        # AQI-level 24h 1..6
        outstr += "&" + "pm25_AQIlvl_24h_co2" + "=" + str(AQIlevel(AQI))
        # same for PM10
        pm10 = float(getfromDict(d,["pm10_co2"]))
        AQI = PM10toAQI(pm10)
        outstr += "&" + "pm10_AQI_co2" + "=" + str(AQI)
        # AQI-level 1..6
        outstr += "&" + "pm10_AQIlvl_co2" + "=" + str(AQIlevel(AQI))
        pm10 = float(getfromDict(d,["pm10_24h_co2"]))
        AQI = PM10toAQI(pm10)
        outstr += "&" + "pm10_AQI_24h_co2" + "=" + str(AQI)
        # AQI-level 24h 1..6
        outstr += "&" + "pm10_AQIlvl_24h_co2" + "=" + str(AQIlevel(AQI))
      except (ValueError, KeyError): pass
      # v0.07: for Ambient AQI calculation
      try:
        pm25 = float(getfromDict(d,["pm25"]))
        AQI = PM25toAQI(pm25)
        outstr += "&" + "pm25_AQI" + "=" + str(AQI)
        # AQI-level 1..6
        outstr += "&" + "pm25_AQIlvl" + "=" + str(AQIlevel(AQI))
        pm25 = float(getfromDict(d,["pm25_24h"]))
        AQI = PM25toAQI(pm25)
        outstr += "&" + "pm25_AQI_24h" + "=" + str(AQI)
        # AQI-level 24h 1..6
        outstr += "&" + "pm25_AQIlvl_24h" + "=" + str(AQIlevel(AQI))
        # same for PM10
        pm10 = float(getfromDict(d,["pm10"]))
        AQI = PM10toAQI(pm10)
        outstr += "&" + "pm10_AQI" + "=" + str(AQI)
        # AQI-level 1..6
        outstr += "&" + "pm10_AQIlvl" + "=" + str(AQIlevel(AQI))
        pm10 = float(getfromDict(d,["pm10_24h"]))
        AQI = PM10toAQI(pm10)
        outstr += "&" + "pm10_AQI_24h" + "=" + str(AQI)
        # AQI-level 24h 1..6
        outstr += "&" + "pm10_AQIlvl_24h" + "=" + str(AQIlevel(AQI))
      except (ValueError, KeyError): pass
    elif what == "windavg":
      try:
        windspeedmph = float(getfromDict(d,["windspeedmph"]))
        winddir = float(getfromDict(d,["winddir"]))
        windgustmph = float(getfromDict(d,["windgustmph"]))
        wind_avg10m.append([int(time.time()),windspeedmph,winddir,windgustmph])
        if "windspdmph_avg10m" not in d.keys():
          outstr += "&windspdmph_avg10m=" + str(avgWind(wind_avg10m,1))
        if "winddir_avg10m" not in d.keys():
          outstr += "&winddir_avg10m=" + str(int(avgWind(wind_avg10m,2)))
        if "windgustmph_max10m" not in d.keys():
          outstr += "&windgustmph_max10m=" + str(maxWind(wind_avg10m,3))
      except (ValueError, KeyError): pass
    elif what == "brightness":
      try:
        sr = float(getfromDict(d,["solarradiation","solarRadiation"]))
        newvalue = round(float(sr) * 126.7,1)
        outstr += "&" + what + "=" + str(newvalue)
      except (ValueError, KeyError): pass
    # v0.08
    elif what == "humidexf":
      try:
        None
        #outstr += "&" + what + "=" + str(newvalue)
      except (ValueError, KeyError): pass
    elif what == "cloudf":
      try:
        tempf = float(getfromDict(d,["tempf"]))
        dewptf = float(getfromDict(d,["dewptf"]))
        cbf = round(((tempf-dewptf) / 4.4) * 1000 + (float(COORD_ALT)*3.28084))
        outstr += "&" + what + "=" + str(cbf)
      except (ValueError, KeyError): pass
    elif what == "sunhours" and int(WS_INTERVAL) <= 60:        # combined way - calculate only if data is available every minute
      try:
        interval = 0
        sunseconds = 0
        sunshine = 0                                           # bool field for sun is shining
        sr = getfromDict(d,["solarradiation","solarRadiation"])
        sunths = float(SUN_MIN) if useSunCalc and COORD_LAT != "" and COORD_LON != "" else 120     # set threshold
        if sr != "null" and float(sr) >= sunths:
          try:
            value = getfromDict(d,["dateutc"])                 # kann auch now (bei WU!) sein - dann aktuelle Zeit nehmen
            currtime = int(time.mktime(time.localtime(int(utcToLocal(time.mktime(time.strptime(value.replace("%20","+").replace("%3A",":"), "%Y-%m-%d+%H:%M:%S")))))))
          except (ValueError, KeyError):
            currtime = time.time()                             # 2do: check if UTC or local
          try:
            lasttime = int(getfromDict(min_max,["last_suncheck"])) # last save time in min_max
          except (ValueError, KeyError):
            lasttime = 0
          min_max["last_suncheck"] = currtime if currtime > 0 else ""
          interval = currtime - lasttime
          if useSunCalc:
            if interval < 365: sunseconds = calcSunduration(sr, interval, currtime)  # gibt Sekunden mit Sonne aus; keine Ahnung, was die 365 soll
            min_max["sunmins"] += sunseconds/60
            if sunseconds > 0:
              min_max["last_suntime"] = currtime if currtime > 0 else ""
              sunshine = 1
          elif interval >= 60 and float(sr) >= sunths:         # only trigger if a minute before
            min_max["sunmins"] += 1
            min_max["last_suntime"] = currtime if currtime > 0 else ""
            sunshine = 1
        # 2do: output sunhours everytime or just if there's a value > 0?
        # if min_max["sunmins"] > 0:
        sunhours = str(round(min_max["sunmins"]/60,2))         # Werner hatte 5 Nachkommastellen? Wieso?
        outstr += "&" + what + "=" + sunhours
        if HIDDEN_FEATURES: outstr += "&" + "sunshine=" + str(sunshine)
      except (ValueError, KeyError): pass
    elif what == "osunhours" and int(WS_INTERVAL) <= 60:       # "old" way - fix threshold of 120W/m² - calculate only if data is available every minute
      try:
        interval = 0
        sr = getfromDict(d,["solarradiation","solarRadiation"])
        try:
          value = getfromDict(d,["dateutc"])                 # kann auch now (bei WU!) sein - dann aktuelle Zeit nehmen
          currtime = int(time.mktime(time.localtime(int(utcToLocal(time.mktime(time.strptime(value.replace("%20","+").replace("%3A",":"), "%Y-%m-%d+%H:%M:%S")))))))
        except (ValueError, KeyError):
          currtime = time.time()                             # 2do: check if UTC or local
        min_max["last_osuncheck"] = currtime if currtime > 0 else ""
        if sr != "null" and float(sr) >= 120:
          try:
            lasttime = int(getfromDict(min_max,["last_osuntime"])) # last save time in min_max
          except (ValueError, KeyError):
            lasttime = 0
          interval = currtime - lasttime
          if interval >= 60 and float(sr) >= 120:              # only trigger if a minute before
            min_max["osunmins"] += 1
            min_max["last_osuntime"] = currtime
        # 2do: output sunhours everytime or just if there's a value > 0?
        #if min_max["osunmins"] > 0:
        sunhours = str(round(min_max["osunmins"]/60,2))
        outstr += "&" + what + "=" + sunhours
      except (ValueError, KeyError): pass
    elif what == "nsunhours":                                  # "new" way - dynamic threshold - calculate sunhours
      try:
        interval = 0
        sunseconds = 0
        sr = getfromDict(d,["solarradiation","solarRadiation"])
        if sr != "null":
          if float(sr) >= float(SUN_MIN):
            try:
              value = getfromDict(d,["dateutc"])               # kann auch now (bei WU!) sein - dann aktuelle Zeit nehmen
              currtime = int(time.mktime(time.localtime(int(utcToLocal(time.mktime(time.strptime(value.replace("%20","+").replace("%3A",":"), "%Y-%m-%d+%H:%M:%S")))))))
            except (ValueError, KeyError):
              currtime = time.time()                           # 2do: check if UTC or local
            try:
              lasttime = int(getfromDict(min_max,["last_nsuncheck"])) # last save time in min_max
            except (ValueError, KeyError):
              lasttime = 0
            interval = currtime - lasttime
            if interval < 365: sunseconds = calcSunduration(sr, interval, currtime)  # gibt Sekunden mit Sonne aus; keine Ahnung, was die 365 soll
            min_max["nsunmins"] += sunseconds/60
            min_max["last_nsuncheck"] = currtime if currtime > 0 else ""
            if sunseconds > 0:
              min_max["last_nsuntime"] = currtime if currtime > 0 else ""
        # 2do: output sunhours everytime or just if there's a value > 0?
        #if min_max["nsunmins"] > 0:
        sunhours = str(round(min_max["nsunmins"]/60,2))        # Werner hatte 5 Nachkommastellen? Wieso?
        outstr += "&" + what + "=" + sunhours
      except (ValueError, KeyError): pass
    elif what == "ptrend":                                     # add pressure items ptrendN & pchangeN
      try:
        val = getfromDict(last_d_m,["ptrend1"])                # attention! last_d_m needed !!!!!!!!!!
        if val != "null": outstr += "&ptrend1="+val
        val = getfromDict(last_d_m,["pchange1"])
        try:
          vnum = hpatoin(float(val),4)
          outstr += "&pchange1="+str(vnum)
        except ValueError: pass
        val = getfromDict(last_d_m,["ptrend3"])
        if val != "null": outstr += "&ptrend3="+val
        val = getfromDict(last_d_m,["pchange3"])
        try:
          vnum = hpatoin(float(val),4)
          outstr += "&pchange3="+str(vnum)
        except ValueError: pass
      except (ValueError, KeyError): pass
    else:
      outstr += "&" + what + "=" + str(newvalue)
    newline = line + outstr
  elif overwrite:
    # gibt es bereits - ueberschreiben?
    for key, value in d.items():
      if key != what:
        newline += "&"+key+"="+value
      elif newvalue == "removefield":
        None
      else:
        newline += "&"+key+"="+str(newvalue)
  else:
    newline = line
  if len(newline) > 0 and newline[0] == "&": newline = newline[1:]
  return newline

def forwardDictToLuftdaten(url,d_in,fwd_sid,fwd_pwd,script,nr,ignoreKeys,remapKeys):
  # 2do: Script-Integration
  d = remappedDict(d_in,remapKeys,nr)                          # remap keys in current dictionary
  pm10value = getfromDict(d,["pm10_ch1","AqPM10","pm10"],ignoreKeys)
  # if there's no such value set to 1 to show at least the pm25-value on map
  if pm10value == "null": pm10value = 1.0
  pm25value = getfromDict(d,["pm25_ch1","AqPM2.5","pm25"],ignoreKeys)
  temperature = getfromDict(d,["tempc"],ignoreKeys)
  humidity = getfromDict(d,["humidity"],ignoreKeys)
  pressure = getfromDict(d,["baromrelhpa"],ignoreKeys)
  if pressure != "null": pressure = round(float(pressure) * 100.0,3)
  pressure_sealevel = getfromDict(d,["baromabshpa"],ignoreKeys)
  if pressure_sealevel != "null": pressure_sealevel = round(float(pressure_sealevel) * 100.0,3)
  ret = ""
  okstr = "<ERROR> "
  # v0.08 multiple attempts httpTries (3)
  v = 0
  while okstr[0:7] == "<ERROR>" and v < httpTries:
    try:
      r = requests.post(url,
        json={
          "software_version": prgname + " " + prgver,
          "sensordatavalues": [
                                {"value_type": "P1", "value": str(pm10value)},
                                {"value_type": "P2", "value": str(pm25value)},
                                {"value_type": "temperature", "value": str(temperature)},
                                {"value_type": "humidity", "value": str(humidity)},
                                {"value_type": "pressure", "value": str(pressure)},
                                {"value_type": "pressure_sealevel", "value": str(pressure_sealevel)}
                              ]
        },
        headers={
          "X-Pin": "1",
          "X-Sensor": fwd_sid,
          "User-Agent": None
        },
        timeout=httpTimeOut
      )
      ret = str(r.status_code)
      okstr = "<ERROR> " if r.status_code not in range(200,203) else ""
      if r.status_code in range(400,500): v = 400
    except requests.exceptions.Timeout as err:
      ret = "TIMEOUT"
    except requests.exceptions.ConnectionError as err:
      ret = "CONNERR"
    except requests.exceptions.RequestException as err:
      ret = "REQERR"
    v += 1                                                     # count of tries
    if v < httpTries and okstr != "": time.sleep(httpSleepTime*v)
  # done
  tries = "" if v == 1 or v > httpTries else " ("+str(v)+" tries)"
  if sndlog: sndPrint(okstr + "FWD-"+nr+": " + url + " sensorID:" + fwd_sid + "=" + str(pm10value) + ", " + str(pm25value) + ", " + str(temperature) + ", " + str(humidity) + ", " + str(pressure) + ", " + str(pressure_sealevel) + " : " + ret + tries)
  return

def convertTemplate(s: str):
  global MSselectlist, enabled, FOSHKrunning, logdir, FWD_URL, FWD_IGNORE, FWD_INTERVAL, FWD_TYPE, fwdtypelist, linkvorlage
  # just for now - 2do
  MSselectlist = ""
  enabled = ""
  FOSHKrunning = wsconnected
  logdir = ""
  FWD_URL = str(fwd_arr[0][0])
  FWD_IGNORE = ", ".join(fwd_arr[0][4])                        # convert the list to string
  FWD_INTERVAL = str(fwd_arr[0][1])
  FWD_TYPE = str(fwd_arr[0][5])
  fwdtypelist = ""
  linkvorlage = ""
  # convert all existing "{" to "{{" and "<!--$" to "{" - but there're still problems with "-->"
  s2 = s.replace("{","{{").replace("}","}}").replace("<!--$","{").replace("-->","}")
  # 2do: does not work before Python v3.6!
  # return eval(f'f"""{s2}"""')
  # for now we use format() instead:
  return s2.format(**globals())

def exchangeTimeString(instr):
  start = instr.find("dateutc=")
  isnow = time.strftime("%Y-%m-%d+%H:%M:%S",time.gmtime())
  if start >= 0:
    ende = instr.find("&",start)
    if ende < 0: ende = len(instr)
    instr = instr.replace(instr[start:ende],"dateutc="+isnow)
  return instr

def EWpostOKstr():
  try:
    offset = (-1*time.timezone)                     # Zeitzone ausgleichen
    if time.localtime()[8]: offset = offset + 3600  # Sommerzeit hinzu
    okstr = "{\"errcode\":\"0\",\"errmsg\":\"ok\",\"UTC_offset\":\"" + str(offset) + "\"}"
  except:
    okstr = "OK\n"
  return okstr

def getKeyFromURL(instr):
  sub = ""
  try:
    start = instr.index("key=")+4
    stop = start
    slen = len(instr)
    while instr[stop] != "?" and instr[stop] != "&":
      stop += 1
      if stop == slen: break
    sub = instr[start:stop]
  except ValueError:
    pass
  return sub

def fixEmptyValue(instr,key,newvalue):
  # v0.07 - repair keys without a value (lightning_time & lightning)
  outstr = instr
  try:
    i = instr.index(key+"=&")
    outstr = instr[:i]+key+"="+newvalue+"&"+instr[i+len(key)+2:] if newvalue != "removefield" else instr[:i]+instr[i+len(key)+2:]
  except ValueError:
    pass
  return outstr

def metricToImpDict(d_in, ignoreKeys, ignoreValues):           # convert given metric dict to imperial dict with imp. keys and values
  d_out = {}                                                   # empty output array to be filled
  for key, value in d_in.items():
    if key in ignoreKeys: None
    else:
      newval = None if value in ignoreValues else strToNum(value)
      if "hpa" in key:
        newkey = key.replace("hpa","in")
        if newval is not None and "time" not in newkey: newval = hpatoin(float(newval),4)
      elif "tempc" in key:
        newkey = key.replace("tempc","tempf")
        if newval is not None and "time" not in newkey: newval = ctof(float(newval),1)
      elif "windchillc" in key:
        newkey = key.replace("windchillc","windchillf")
        if newval is not None and "time" not in newkey: newval = ctof(float(newval),1)
      elif "heatindexc" in key:
        newkey = key.replace("heatindexc","heatindexf")
        if newval is not None and "time" not in newkey: newval = ctof(float(newval),1)
      elif "feelslikec" in key:
        newkey = key.replace("feelslikec","feelslikef")
        if newval is not None and "time" not in newkey: newval = ctof(float(newval),1)
      elif "dewptc" in key:
        newkey = key.replace("dewptc","dewptf")
        if newval is not None and "time" not in newkey: newval = ctof(float(newval),1)
      elif "tempinc" in key:
        newkey = key.replace("tempinc","tempinf")
        if newval is not None and "time" not in newkey: newval = ctof(float(newval),1)
      elif key.startswith("temp") and key[5] == "c":
        newkey = "temp" +key[4] + "f" + key[6:]
        if newval is not None and "time" not in newkey: newval = ctof(float(newval),1)
      elif "tc_co2" in key:
        newkey = key.replace("tc_co2","tf_co2")
        if newval is not None and "time" not in newkey: newval = ctof(float(newval),1)
      elif "kmh" in key:
        newkey = key.replace("kmh","mph")
        if newval is not None and "time" not in newkey: newval = kmhtomph(float(newval),2)
      elif "rain" in key and "mm" in key:
        newkey = key.replace("mm","in")
        if newval is not None and "time" not in newkey: newval = mmtoin(float(newval),3)
      else: newkey = key
      d_out.update({newkey : newval})
  return d_out    

def addStatusToDict(d, makeBool=False):                        # add Status to dict as 0/1 or True/False (if makeBool
  # add warnings & states
  func = bool if makeBool else str
  d.update({"running" : func(int(wsconnected))})
  d.update({"wswarning" : func(int(inWStimeoutWarning))})
  d.update({"sensorwarning" : func(int(inSensorWarning))})
  if inSensorWarning and SensorIsMissed != "": d.update({"missed" : SensorIsMissed})
  d.update({"batterywarning" : func(int(inBatteryWarning))})
  d.update({"stormwarning" : func(int(inStormWarning))})
  d.update({"tswarning" : func(int(inTSWarning))})
  d.update({"updatewarning" : func(int(updateWarning))})
  d.update({"leakwarning" : func(int(inLeakageWarning))})
  d.update({"co2warning" : func(int(inCO2Warning))})
  d.update({"intvlwarning" : func(int(inIntervalWarning))})
  d.update({"time" : strToNum(loxTime(time.time()))})
  return d

def getStatusString(sep, makeBool=False):                       # output status string
  sw_what = sep+"missed=" + SensorIsMissed if inSensorWarning and SensorIsMissed != "" else ""
  if makeBool:
    s = "running=" + str(wsconnected) + sep + "wswarning=" + str(inWStimeoutWarning) + sep + "sensorwarning=" + str(inSensorWarning) + sw_what + sep + "batterywarning=" + str(inBatteryWarning) + sep + "stormwarning=" + str(inStormWarning) + sep + "tswarning=" + str(inTSWarning) + sep + "updatewarning=" + str(updateWarning) + sep + "leakwarning=" + str(inLeakageWarning) + sep + "co2warning=" + str(inCO2Warning) + sep + "intvlwarning=" + str(inIntervalWarning) + sep + "time=" + str(loxTime(time.time()))
  else:
    s = "running=" + str(int(wsconnected)) + sep + "wswarning=" + str(int(inWStimeoutWarning)) + sep + "sensorwarning=" + str(int(inSensorWarning)) + sw_what + sep + "batterywarning=" + str(int(inBatteryWarning)) + sep + "stormwarning=" + str(int(inStormWarning)) + sep + "tswarning=" + str(int(inTSWarning)) + sep + "updatewarning=" + str(int(updateWarning)) + sep + "leakwarning=" + str(int(inLeakageWarning)) + sep + "co2warning=" + str(int(inCO2Warning)) + sep + "intvlwarning=" + str(inIntervalWarning) + sep + "time=" + str(loxTime(time.time()))
  return s

def tableRow(what, options, comment, myIP, myPort):
  s = o = ""
  link = "http://"+myIP+":"+myPort+what
  s += "<tr><td><a href=\""+link+"\" target=\"_blank\">"+link+"</a></td><td>"+options+"<td>"+comment+"</td></tr>\n"
  return s

def instrReplace(s):
  # convert the new rain key names of a WS90 to the "old" ones - only applies if the WS90 is the only sensor ("old" names not present already)
  # configurable with Weatherstation\WS90_AUTO = True/False (True: convert these keys; False: keep untouched)
  if WS90_CONVERT and "rainratein" not in s: s.replace("rrain_piezo","rainratein")
  if WS90_CONVERT and "eventrainin" not in s: s.replace("erain_piezo","eventrainin")
  if WS90_CONVERT and "hourlyrainin" not in s: s.replace("hrain_piezo","hourlyrainin")
  if WS90_CONVERT and "dailyrainin" not in s: s.replace("drain_piezo","dailyrainin")
  if WS90_CONVERT and "weeklyrainin" not in s: s.replace("wrain_piezo","weeklyrainin")
  if WS90_CONVERT and "monthlyrainin" not in s: s.replace("mrain_piezo","monthlyrainin")
  if WS90_CONVERT and "yearlyrainin" not in s: s.replace("yrain_piezo","yearlyrainin")
  return s

class RequestHandler(BaseHTTPRequestHandler):
  # probably the correct position for timeout
  timeout = 5
  close_connection = True
  server_version = prgname+"/"+prgver+" "+BaseHTTPRequestHandler.server_version
  protocol_version = 'HTTP/1.1'

  def do_GET(self):
    request_path = self.path
    request_addr = self.client_address[0]
    instr = request_path
    global myDebug                                           # global to change for all
    global LEAK_WARNING, CO2_WARNING, INTVL_WARNING, BUT_PRINT          # it does not work without
    # check authentication
    if AUTH_PWD != "" and AUTH_PWD not in request_path and request_path != "/FOSHKplugin/state":
      logPrint("<INFO> unauthorized get-request from " + str(request_addr) + ": " + str(request_path))
    # Lieferung im WU-Format
    elif "updateweatherstation" in request_path or "endpoint" in request_path or "/data/report/" in request_path:
      # in v4.2.8 the path is automatically set to /data/report/ without a ? - so fix this first
      instr = instr.replace("/data/report/","?")
      # eingehender String ist interessant von ? bis Leerzeichen
      try:
        payload_start = instr.index("?")+1
      except ValueError:
        payload_start = 0
      if payload_start > 0: payload_start+1
      instr = instr[payload_start:]
      global last_RAWstr
      last_RAWstr = instr

      # v0.09: count real interval time
      global lastData
      global inIntervalWarning
      now = int(time.time())
      if lastData > 0:
        inttime = now - lastData
        intervald.append(inttime)
        l = len(intervald)
        isinterval = int((sum(intervald)-min(intervald)-max(intervald))/(l-2)) if l > 2 else int(sum(intervald)/l)
        #print("isintvl: "+str(inttime)+" l: "+str(l)+" isintvl10: "+str(isinterval)+" all: "+str(intervald))
        if EVAL_VALUES and len(instr) > 0: instr +="&isintvl="+str(inttime)+"&isintvl10="+str(isinterval)

        if INTVL_WARNING:                                              # warn if measured interval is more than 10% above the agreed send interval
          if isinterval > INTVL_LIMIT and not inIntervalWarning:
            logPrint("<WARNING> real sending interval ("+str(isinterval)+") mismatches the interval set to the weather station ("+str(WS_INTERVAL)+")")
            sendUDP("SID=" + defSID + " intvlwarning=1 time=" + str(loxTime(time.time())))
            pushPrint("<WARNING> real sending interval ("+str(isinterval)+") mismatches the interval set to the weather station ("+str(WS_INTERVAL)+")")
            inIntervalWarning = True
          elif isinterval <= INTVL_LIMIT and inIntervalWarning:        # cancel warning if value is below the warning threshold
            logPrint("<OK> real sending interval ("+str(isinterval)+") matches the interval set to the weather station ("+str(WS_INTERVAL)+") again")
            sendUDP("SID=" + defSID + " intvlwarning=0 time=" + str(loxTime(time.time())))
            pushPrint("<OK> real sending interval ("+str(isinterval)+") matches the interval set to the weather station ("+str(WS_INTERVAL)+") agaim")
            inIntervalWarning = False
      lastData = now

      # v0.06: possibly fake the outdoor-sensor with internal values
      if fakeOUT_TEMP != "": instr = instr.replace("&"+fakeOUT_TEMP+"=","&tempf=")
      if fakeOUT_HUM != "": instr = instr.replace("&"+fakeOUT_HUM+"=","&humidity=")

      # v0.07: if configure via Export\OUT_TIME (exchangeTime) replace incoming time string with time string of receipt
      if exchangeTime:
        instr = exchangeTimeString(instr)

      # hier ggf. um weitere Felder ergaenzen - etwa dewpt, windchill und feelslike
      #global EVAL_VALUES
      if EVAL_VALUES:
        # erzeugt Wertepaar mit Namen "feld",Wert,Overwrite existent
        instr = addDataToLine(instr,"dewptf",None,False)
        instr = addDataToLine(instr,"windchillf",None,False)
        instr = addDataToLine(instr,"feelslikef",None,False)
        instr = addDataToLine(instr,"heatindexf",None,False)
        instr = addDataToLine(instr,"pm25_AQI",None,False)
        instr = addDataToLine(instr,"windavg",None,False)
        instr = addDataToLine(instr,"brightness",None,False)
        instr = addDataToLine(instr,"cloudf",None,False)
        instr = addDataToLine(instr,"sunhours",None,False)     # combined procedure - dependend on SUN_CALC and existence of lat/lon
        if HIDDEN_FEATURES:                                    # for testing: should be removed in next release
          instr = addDataToLine(instr,"osunhours",None,False)  # old procedure with fixes threshold of 120W/m²
          instr = addDataToLine(instr,"nsunhours",None,False)  # new procedure with dynamic threshold
      if FIX_LIGHTNING and last_lightning_time != 0:
        # set empty keys to last known values
        instr = fixEmptyValue(instr,"lightning_time",str(last_lightning_time))
        instr = fixEmptyValue(instr,"lightning",str(last_lightning))
      global ADD_ITEMS
      # add additional fields (like lat, lon, alt, neighborhood, country or qcStatus)
      if ADD_ITEMS != "":
        if ADD_ITEMS[0] != "&": ADD_ITEMS = "&" + ADD_ITEMS
        instr += ADD_ITEMS

      # falls das Ende des Strings durch ein Leerzeichen definiert ist
      #payload_ende = instr.index(" ")
      #instr = instr[:payload_ende]

      if rawlog: rawlogger.info(hidePASSKEY(instr))

      # v0.09 possibility to adjust the instr globally - for WS90 compatibility
      # rrain_piezo, erain_piezo, hrain_piezo, drain_piezo, wrain_piezo, mrain_piezo, yrain_piezo
      instr = instrReplace(instr)

      # v0.09 also exchange the keys in last_RAWstr
      last_RAWstr = instrReplace(last_RAWstr)

      # create dictionaries E = Imperial; M = Metric; R = RAW
      d_e = stringToDict(instr,"&")
      d_r = stringToDict(last_RAWstr,"&")
      d_m = convertDictToMetricDict(d_e,IGNORE_EMPTY,LOX_TIME)

      # v0.08 fill the min/max array
      # fill min_max with metric or empire values?
      generateMinMax(d_m)                                      # fill minmax and send via UDP

      global last_d_e
      last_d_e = d_e
      global last_d_m
      last_d_m = d_m

      # v0.09 one global list for all (except status) lists
      global last_d_all
      last_d_all = last_d_e.copy()
      last_d_all.update(last_d_m)
      last_d_all.update(min_max)
      last_d_all.update(metricToImpDict(min_max,[],["null"]))

      # v0.08 add ptrend1, pchange1, ptrend3 & pchange3 - needs d_m and is for instr only
      if EVAL_VALUES:
        instr = addDataToLine(instr,"ptrend",None,False)

      # zerlegen
      UDPstr = "SID=" + defSID + " " + dictToString(d_m," ",True,UDP_IGNORE) if USE_METRIC else "SID=" + defSID + " " + dictToString(d_e," ",True,UDP_IGNORE)

      # jetzt UDPstr versenden
      sendUDP(UDPstr)

      # fuer weitere Anfragen merken
      global last_csv_time
      # letzte Meldung der Wetterstation merken
      global last_ws_time
      last_ws_time = int(time.time())
      
      # for GW1000/DP1500 no response needed; but in forward-mode (myself) this is a must
      OKanswer = "OK\n"
      try:
        self.send_response(200)
        self.send_header('Content-Type','text/html')
        self.send_header('Content-Length',str(len(OKanswer)))
        self.send_header('Connection','Close')
        self.end_headers()
      except:
        if myDebug: logPrint("<DEBUG> except in header-response in do_GET")
        pass
      # v0.07 always reply OK for Ambient-compatibility
      try:
        self.wfile.write(bytearray(OKanswer,OutEncoding))
      except:
        if myDebug: logPrint("<DEBUG> except in wfile.write 1 in do_GET")
        pass
      # String nach WU-String umwandeln und an alle FWD_URL im gesetzen Intervall versenden
      if forwardMode:
        for i in range(len(fwd_arr)):                          # 0:url,1:interval,2:interval_num,3:last,4:ignore,5:type,6:fwd_sid,7:fwd_pwd,8:status,9:minmax,10:script,11:nr,12:mqttcycle,13:fwd_remap
          if time.time() >= fwd_arr[i][3]+fwd_arr[i][2]:
            if fwd_arr[i][5] == "WU":                          # String nach WU wandeln und per get versenden
              t = threading.Thread(target=forwardStringToWU, args=(fwd_arr[i][0],instr,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "RAW":                       # RAW-Dict ohne Aenderung per get weitersenden
              t = threading.Thread(target=forwardDictToHTTP, args=(fwd_arr[i][0],d_r,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],False,True,False,"&"))
              t.start()
            elif fwd_arr[i][5] == "EW":                        # eingehenden, erweiterten String nach Ecowitt wandeln und per post versenden
              t = threading.Thread(target=forwardStringToEW, args=(fwd_arr[i][0],instr,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] in ("RAWEW","EWRAW"):           # eingehenden RAW-String nach Ecowitt wandeln und per post versenden
              t = threading.Thread(target=forwardStringToEW, args=(fwd_arr[i][0],last_RAWstr,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "LD":                        # forward pm25 value only to luftdaten.info; args: url, fwd_sid, wert
              t = threading.Thread(target=forwardDictToLuftdaten, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],))
              t.start()
            elif fwd_arr[i][5] == "UDP":                       # forward metr. or imp. dict per UDP (other target than Loxone)
              d_fwd = d_m if USE_METRIC else d_e
              t = threading.Thread(target=forwardDictToUDP, args=(fwd_arr[i][0],d_fwd,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]," "))
              t.start()
            elif fwd_arr[i][5] in ("RAWUDP","UDPRAW"):         # forward incoming string via UDP
              t = threading.Thread(target=forwardStringToUDP, args=(fwd_arr[i][0],instr,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] in ("EWUDP","UDPEW"):           # forward imp. dict per UDP (convert to EW-format)
              t = threading.Thread(target=forwardDictToUDP, args=(fwd_arr[i][0],d_e,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],"&"))
              t.start()
            elif fwd_arr[i][5] in ("RAWCSV","CSVRAW"):         # forward the raw values as CSV-string for e.g. Edomi
              t = threading.Thread(target=forwardDictToHTTP, args=(fwd_arr[i][0],d_r,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],True,True,False,";"))
              t.start()
            elif fwd_arr[i][5] == "CSV":                       # forward as CSV-string for e.g. Edomi
              d_fwd = d_m if USE_METRIC else d_e
              t = threading.Thread(target=forwardDictToHTTP, args=(fwd_arr[i][0],d_fwd,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],True,True,False,";"))
              t.start()
            elif fwd_arr[i][5] == "AMB":                       # convert incoming string to Ambient and send via GET
              t = threading.Thread(target=forwardStringToAMB, args=(fwd_arr[i][0],instr,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] in ("RAWAMB","AMBRAW"):         # convert incoming RAW-string to Ambient and send via GET
              t = threading.Thread(target=forwardStringToAMB, args=(fwd_arr[i][0],last_RAWstr,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "MT":                        # convert metric dict to Meteotemplate and send via GET
              t = threading.Thread(target=forwardDictToMeteoTemplate, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "WC":                        # convert metric dict to WeatherCloud and send via GET
              t = threading.Thread(target=forwardDictToWC, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "AWEKAS":                    # convert metric dict to Awekas-API and send via GET
              t = threading.Thread(target=forwardDictToAwekas, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "WETTERCOM":                 # convert metric dict to wetter.com-API and send via GET
              t = threading.Thread(target=forwardDictToWetterCOM, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "WEATHER365":                # convert metric dict to Weather365-API and send via POST
              t = threading.Thread(target=forwardDictToWeather365, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "WETTERSEKTOR":              # convert metric dict to Wettersektor-API via POST
              t = threading.Thread(target=forwardDictToWetterSektor, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "MQTTMET":                   # send metric dict to MQTT server
              t = threading.Thread(target=forwardDictToMQTT, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],fwd_arr[i][12],True))
              t.start()
            elif fwd_arr[i][5] == "MQTTIMP":                   # send imperial dict to MQTT server
              t = threading.Thread(target=forwardDictToMQTT, args=(fwd_arr[i][0],d_e,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],fwd_arr[i][12],False))
              t.start()
            elif fwd_arr[i][5] == "INFLUXMET":                 # send metric dict to InfluxDB server
              t = threading.Thread(target=forwardDictToInfluxDB, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],True))
              t.start()
            elif fwd_arr[i][5] == "INFLUXIMP":                 # send imperial dict to InfluxDB server
              t = threading.Thread(target=forwardDictToInfluxDB, args=(fwd_arr[i][0],d_e,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],False))
              t.start()
            elif fwd_arr[i][5] in ("REALTIMETXT","CLIENTRAWTXT","CSVFILE","TXTFILE","TEXTFILE","RAWTEXT","WSWIN"):      # convert dict to file
              d_fwd = d_e if fwd_arr[i][5] == "RAWTEXT" else d_m                                                        # use imperial dict for RAWTEXT only
              t = threading.Thread(target=forwardDictToFile, args=(fwd_arr[i][0],d_fwd,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],fwd_arr[i][5]))
              t.start()
            elif fwd_arr[i][5] == "APRS":                      # convert imperial dict to APRS and send via TCP/IP
              t = threading.Thread(target=forwardDictToAPRS, args=(fwd_arr[i][0],d_e,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            else:                                              # metr. oder imperiales dict wie UDP-String per get versenden
              d_fwd = d_m if USE_METRIC else d_e
              t = threading.Thread(target=forwardDictToHTTP, args=(fwd_arr[i][0],d_fwd,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],False,True,True,"&"))
              t.start()
            fwd_arr[i][3] = time.time()
      if CSVsave and time.time() >= last_csv_time + CSV_INTERVAL_num:
        if last_csv_time == 0:
          hname = "/tmp/"+prgname+"-"+LBH_PORT+".csvheader"
          try:
            hfile = open(hname,"w+")
            d_fwd = d_m if USE_METRIC else d_e
            hfile.write(dictToString(d_fwd,";",True,[],[],True,False))
            hfile.close()
            logPrint("<OK> CSV-header-file " + hname + " written")
          except:
            logPrint("<ERROR> unable to write CSV-header-file to " + hname + "!")
            pass
        csvline = lineToCSV(d_m,CSV_FIELDS) if USE_METRIC else lineToCSV(d_e,CSV_FIELDS)
        try:
          fcsv.write(csvline + "\r\n")
          fcsv.flush()
        except:
          sndPrint("<ERROR> unable to write the record to " + CSV_NAME + "!",True)
          pass
        if sndlog: sndPrint("CSV: " + csvline)
        last_csv_time = time.time()
    else:
      # Anfragen von Weather4Loxone etc. beantworten
      try:
        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.send_header('Connection','Close')
        self.end_headers()
      except:
        if myDebug: logPrint("<DEBUG> except in header-response etc. in do_GET")
        pass
      ignoreValues=["-9999","None","null"]
      # v0.09: allow http auto refresh via refresh=refreshtime
      refreshtime = getURLvalue(request_path,"REFRESH")
      if refreshtime != "":
        try:
          refreshtime = int(refreshtime)
          htmlout = "<!DOCTYPE html>\n"
          htmlout += "<html>\n<head>\n<title>"+prgname+" "+prgver+"</title>\n"
          htmlout += "<meta name=\"viewport\" content=\"width=device-width, initial-scale\=1.0\">\n"
          htmlout += "<link rel=\"icon\" type=\"image/png\" href=\"data:image/png;base64,AAABAAEAEBAAAAEAIABoBAAAFgAAACgAAAAQAAAAIAAAAAEAIAAAAAAAAAQAAMMOAADDDgAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAFAAAABgAAAAYAAAADAAAAA0CbUw9QxGoJUMNpBVDDaRpQw2kOUMNpAlDDaQBQw2kAAAAAAAAAAAUAAAA7AAAAQwAAAFMAAABiAAAAVAAAAF0wdT9YUcZrYlDDaVlQw2lwUMNpZlDDaUZQw2kHUMNpAAAAAAAAAAADAAAAPAAAADwECgVDCRYMTwwdEEQZPiFrK2o5OFLHazlQw2kuUMNpJVDDaTBQw2kkUMNpA1DDaQAAAAAABQUFAE3zcQBRqGQDUcVqZVHFaoJRxmt3UcVqrlDDaYZQw2mXUMNpm1DDaTtQw2kQUMNpAFDDaQAAAAAAAAAAAFDDaQBQw2kAUMNpXVDDab5Qw2mZUMNpIVDDaXlQw2lzUMNpiFDDaX1Qw2lzUMNpfVDDaQ5Qw2kAAAAAAAAAAABQw2kAUMNpAFDDaYBQw2nVUMNpuVDDaRlQw2lxUMNpe1DDaSlQw2kAUMNpAlDDaXJQw2kbUMNpAAAAAAAAAAAAUMNpAFDDaQBQw2l/UMNp1FDDabhQw2kZUMNpclDDaZBQw2mEUMNpaFDDaWxQw2miUMNpH1DDaQAAAAAAAAAAAFDDaQBQw2kAUMNpfVDDadNQw2m1UMNpGFDDaXFQw2liUMNpqFDDaaVQw2moUMNpiVDDaQxQw2kAAAAAAAAAAABQw2kAUMNpAFDDaSlQw2lYUMNpRFDDaQpQw2l1UMNpFlDDaQ5Qw2kPUMNpD1DDaQZQw2kAUMNpAAAAAAAAAAAAAAAAAAAAAABQw2kAUMNpF1DDaX5Qw2lfUMNppVDDaVdQw2mCUMNpKlDDaQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABQw2kAUMNpAFDDaStQw2mzUMNpolDDabFQw2mUUMNpuFDDaUJQw2kAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAUMNpAFDDaSlQw2mIUMNpV1DDaR5Qw2mLUMNpJFDDaUJQw2kzUMNpA1DDaQAAAAAAAAAAAAAAAAAAAAAAAAAAAFDDaQBQw2k4UMNpgFDDaZhQw2mCUMNpl1DDaXpQw2mWUMNpxlDDaSpQw2kAAAAAAAAAAAAAAAAAAAAAAAAAAABQw2kAUMNpN1DDaZ5Qw2lpUMNpElDDaQxQw2kMUMNpIlDDaTRQw2kGUMNpAAAAAAAAAAAAAAAAAAAAAAAAAAAAUMNpAFDDaTBQw2mIUMNpD1DDaQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFDDaQBQw2kGUMNpClDDaQBQw2kAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAEAAIABAACAAQAA4AcAAOADAADgIwAA4AMAAOADAADgBwAA8B8AAPAfAADgDwAA4A8AAOAPAADj/wAA5/8AAA==\">\n"
          htmlout += "<meta http-equiv=\"Expires\" content=\"-1\" />\n"
          htmlout += "<meta http-equiv=\"refresh\" content=\""+str(refreshtime)+"\" />\n"
          htmlout += "</head>\n<body>\n"
        except: 
          refreshtime = ""
          pass
      else: htmlout = ""
      if "CSVHDR" in request_path:
        if "units=" in request_path: d_out = last_d_m if "units=m" in request_path else last_d_e
        else: d_out = last_d_m if USE_METRIC else last_d_e
        sep = getSeparator(request_path)                       # separator from url or CSV_FIELDS
        htmlout += dictToString(d_out,sep,True,[],[],True,False)
      # v0.07 now output empty fields also (needs CSV\CSV_FIELDS in config file)
      elif "SSVHDR" in request_path:
        sep = getSeparator(request_path)                       # separator from url or CSV_FIELDS
        htmlout += "time"+sep+CSV_FIELDS.replace(";",sep)
      elif "UDP" in request_path:
        if "units=" in request_path: d_out = last_d_m if "units=m" in request_path else last_d_e
        else: d_out = last_d_m if USE_METRIC else last_d_e
        sep = getSeparator(request_path," ")                   # separator from url or CSV_FIELDS
        htmlout += dictToString(d_out,sep,True)
        if "minmax" in request_path:
          if htmlout != "": htmlout += sep
          if ("units=" in request_path and "units=m" not in request_path) or not USE_METRIC:
            # ensure to convert min_max values to imperial metricToImpDict
            htmlout += dictToString(metricToImpDict(min_max,[],["null"]),sep,False,[],ignoreValues,True,True,True)
          else:
            htmlout += dictToString(min_max,sep,False,[],["null"],True,True,True)
        if "status" in request_path:
          sw_what = " missed=" + SensorIsMissed if inSensorWarning and SensorIsMissed != "" else ""
          if htmlout != "": htmlout += sep
          if "bool" in request_path:
            htmlout += "running=" + str(wsconnected) + sep + "wswarning=" + str(inWStimeoutWarning) + sep + "sensorwarning=" + str(inSensorWarning) + sw_what + sep + "batterywarning=" + str(inBatteryWarning) + sep + "stormwarning=" + str(inStormWarning) + sep + "tswarning=" + str(inTSWarning) + sep + "updatewarning=" + str(updateWarning) + sep + "leakwarning=" + str(inLeakageWarning) + sep + "co2warning=" + str(inCO2Warning) + sep + "intvlwarning=" + str(inIntervalWarning) + sep + "time=" + str(loxTime(time.time()))
          else:
            htmlout += "running=" + str(int(wsconnected)) + sep + "wswarning=" + str(int(inWStimeoutWarning)) + sep + "sensorwarning=" + str(int(inSensorWarning)) + sw_what + sep + "batterywarning=" + str(int(inBatteryWarning)) + sep + "stormwarning=" + str(int(inStormWarning)) + sep + "tswarning=" + str(int(inTSWarning)) + sep + "updatewarning=" + str(int(updateWarning)) + sep + "leakwarning=" + str(int(inLeakageWarning)) + sep + "co2warning=" + str(int(inCO2Warning)) + sep + "intvlwarning=" + str(int(inIntervalWarning)) + sep + "time=" + str(loxTime(time.time()))
      elif "JSON" in request_path:                             # as JSON with options boolstatus
        sep = getSeparator(request_path)
        if "units=" in request_path: d_in = last_d_m if "units=m" in request_path else last_d_e
        else: d_in = last_d_m if USE_METRIC else last_d_e
        d_out = {}
        for key, value in d_in.items():
          newval = None if value in ignoreValues else strToNum(value)
          d_out.update({key : newval})
        if "minmax" in request_path:                           # add minmax values
          if ("units=" in request_path and "units=m" not in request_path) or not USE_METRIC:
            d_out.update(metricToImpDict(min_max,[],ignoreValues))
          else:
            for key, value in min_max.items():
              newval = None if value in ignoreValues else strToNum(value)
              d_out.update({key : newval})
        if "status" in request_path:                           # add status values
          d_out = addStatusToDict(d_out, "bool" in request_path)
        htmlout += json.dumps(d_out)
      elif "STRING" in request_path:
        sep = getSeparator(request_path, ";")
        if "units=" in request_path: d_out = last_d_m if "units=m" in request_path else last_d_e
        else: d_out = last_d_m if USE_METRIC else last_d_e
        htmlout += dictToString(d_out,sep,True)
        if "minmax" in request_path:
          if htmlout != "": htmlout += sep
          if ("units=" in request_path and "units=m" not in request_path) or not USE_METRIC:
            htmlout += dictToString(metricToImpDict(min_max,[],["null"]),sep,False,[],ignoreValues,True,True,True)
          else:
            htmlout += dictToString(min_max,sep,False,[],["null"],True,True,True)
        if "status" in request_path:
          if htmlout != "": htmlout += sep
          htmlout += getStatusString(sep, "bool" in request_path)
      # v0.08 realtime.txt
      elif "REALTIMETXT" in request_path or "REALTIME.TXT" in request_path or "realtime.txt" in request_path:
        htmlout += dictToREALTIME(last_d_m,"",{},{})
      # v0.08 realtime.txt
      elif "CLIENTRAWTXT" in request_path or "CLIENTRAW.TXT" in request_path or "clientraw.txt" in request_path:
        htmlout += dictToCLIENTRAW(last_d_m,"",{},{})
      elif "WSWIN" in request_path:
        htmlout += dictToWSWin(last_d_m,"",{},{})
      elif "CSVFILE" in request_path or "FOSHKplugin.csv" in request_path or "foshkplugin.csv" in request_path or "TXTFILE" in request_path or "TEXTFILE" in request_path or "FOSHKplugin.txt" in request_path or "foshkplugin.txt" in request_path:
        if "units=" in request_path: d_out = last_d_m if "units=m" in request_path else last_d_e
        else: d_out = last_d_m if USE_METRIC else last_d_e
        d = d_out.copy()
        # add minmax values
        if "minmax" in request_path: d.update(min_max)
        if "status" in request_path:
          d = addStatusToDict(d, "bool" in request_path)
        if "CSVFILE" in request_path or "FOSHKplugin.csv" in request_path or "foshkplugin.csv" in request_path:
          sep = getSeparator(request_path,";")                                 # separator from url or CSV_FIELDS
          htmlout += dictToString(d,sep,True,[],[],True,True,False)            # output as csv, separated with ";"
        else:
          sep = getSeparator(request_path,"\n")                                # separator from url or CSV_FIELDS
          htmlout += dictToString(d,sep,False,[],[],True,True,False)           # output as txt, separated with "\n"
      elif "CSV" in request_path:
        if "units=" in request_path: d_out = last_d_m if "units=m" in request_path else last_d_e
        else: d_out = last_d_m if USE_METRIC else last_d_e
        htmlout += dictToString(d_out,",",True,[],[],False)
      elif "SSV" in request_path:
        if "units=" in request_path: d_out = last_d_m if "units=m" in request_path else last_d_e
        else: d_out = last_d_m if USE_METRIC else last_d_e
        htmlout += lineToCSV(d_out,CSV_FIELDS)
        htmlout += dictToString(min_max,";",False,[],["null"],False,True,True)
        sep = getSeparator(request_path,";")                                   # separator from url or CSV_FIELDS
        if sep != ";": htmlout = htmlout.replace(";",sep)
      elif "RAW" in request_path:
        htmlout += last_RAWstr
        sep = getSeparator(request_path,"&")                   # separator from url or CSV_FIELDS
        if sep != "&": htmlout = htmlout.replace("&",sep)
      elif "APRS" in request_path:
        fwd_sid = getURLvalue(request_path,"USER")
        fwd_sid = "DUMMY" if fwd_sid == "" else fwd_sid
        htmlout += dictToAPRS(last_d_e,fwd_sid,[],[])
      # v0.07 Einzelabfrage von Werten
      elif "getvalue" in request_path:
        if "units=e" in request_path:
          d = last_d_e.copy()
          imperial = True
        else:
          d = last_d_m.copy()
          imperial = False
        d = addStatusToDict(d, "bool" in request_path)         # append status to the dict d if set
        d.update(min_max)                                      # add minmax values
        d.update(metricToImpDict(min_max,[],ignoreValues))     # min_max as imperial values & names
        # parsen key=
        key = getKeyFromURL(request_path)
        val = str(getfromDict(d,[getKeyFromURL(request_path)]))
        # auto
        if val == "null": val = str(getfromDict(last_d_m,[getKeyFromURL(request_path)])) if imperial else str(getfromDict(last_d_e,[getKeyFromURL(request_path)]))
        htmlout += "" if val == "null" else val
      elif "observations" in request_path and "current" in request_path and "json" in request_path and "units=e" in request_path:
        htmlout += dictToWUServer(last_d_e,"&",False)
      elif "observations" in request_path and "current" in request_path and "json" in request_path and "units=m" in request_path:
        htmlout += dictToWUServer(last_d_m,"&",True)
      elif "w4l/current.dat" in request_path:
        htmlout += DictToW4L(last_d_m," ", True)
      elif "DATA" in request_path:                             # 2do: wofuer hatte ich das gedacht? was: "/FOSHKplugin"
        sep = getSeparator(request_path, " ")
        if "units=" in request_path: d_out = last_d_m if "units=m" in request_path else last_d_e
        else: d_out = last_d_m if USE_METRIC else last_d_e
        htmlout += dictToString(d_out,sep,True)
        if "minmax" in request_path:
          if htmlout != "": htmlout += sep
          if ("units=" in request_path and "units=m" not in request_path) or not USE_METRIC:
            htmlout += dictToString(metricToImpDict(min_max,[],["null"]),sep,False,[],ignoreValues,True,True,True)
          else:
            htmlout += dictToString(min_max,sep,False,[],["null"],True,True,True)
        if "status" in request_path:
          if htmlout != "": htmlout += sep
          htmlout += getStatusString(sep, "bool" in request_path)
      elif "/FOSHKplugin/state" in request_path:
        htmlout += str("running")
      elif "/FOSHKplugin/status" in request_path:
        #sw_what = " missed=" + SensorIsMissed if inSensorWarning and SensorIsMissed != "" else ""
        #htmlout = "running=" + str(int(wsconnected)) + " wswarning=" + str(int(inWStimeoutWarning)) +  " sensorwarning=" + str(int(inSensorWarning)) + sw_what + " batterywarning=" + str(int(inBatteryWarning)) + " stormwarning=" + str(int(inStormWarning)) + " tswarning=" + str(int(inTSWarning)) + " updatewarning=" + str(int(updateWarning)) + " leakwarning=" + str(int(inLeakageWarning)) + " co2warning=" + str(int(inCO2Warning)) + " intvlwarning=" + str(int(inIntervalWarning)) + " time=" + str(loxTime(time.time()))
        sep = getSeparator(request_path, " ")
        htmlout += getStatusString(sep, "bool" in request_path)
      elif "/FOSHKplugin/minmax" in request_path:
        htmlout += dictToString(min_max," ",True)
      elif "/FOSHKplugin/LBU_PORT" in request_path:
        htmlout = LBU_PORT
      elif "/FOSHKplugin/patchW4L" in request_path:
        foshkdatadir = checkLBP_PATH(SVC_NAME,"lbpdatadir")
        htmlout = str(os.popen(foshkdatadir+"/foshkplugin.py -patchW4L").read()).replace("\n","<br/>")
      elif "/FOSHKplugin/recoverW4L" in request_path:
        foshkdatadir = checkLBP_PATH(SVC_NAME,"lbpdatadir")
        htmlout = str(os.popen(foshkdatadir+"/foshkplugin.py -recoverW4L").read()).replace("\n","<br/>")
      elif "/FOSHKplugin/debug=enable" in request_path:
        myDebug = True
        logPrint("<INFO> debug mode via http/get enabled from " + request_addr)
        htmlout = "debug mode enabled"
      elif "/FOSHKplugin/debug=disable" in request_path:
        myDebug = False
        logPrint("<INFO> debug mode via http/get disabled from " + request_addr)
        htmlout = "debug mode disabled"
      elif "/FOSHKplugin/leakwarning=enable" in request_path:
        LEAKAGE_WARNING = True
        logPrint("<INFO> leakwarning via http/get enabled from " + request_addr)
        htmlout = "leakwarning enabled"
      elif "/FOSHKplugin/leakwarning=disable" in request_path:
        LEAKAGE_WARNING = False
        logPrint("<INFO> leakwarning via http/get disabled from " + request_addr)
        htmlout = "leakwarning disabled"
      elif "/FOSHKplugin/co2warning=enable" in request_path:
        CO2_WARNING = True
        logPrint("<INFO> co2warning via http/get enabled from " + request_addr)
        htmlout = "co2warning enabled"
      elif "/FOSHKplugin/co2warning=disable" in request_path:
        CO2_WARNING = False
        logPrint("<INFO> co2warning via http/get disabled from " + request_addr)
        htmlout = "co2warning disabled"
      elif "/FOSHKplugin/intvlwarning=enable" in request_path:
        INTVL_WARNING = True
        logPrint("<INFO> intervalwarning via http/get enabled from " + request_addr)
        htmlout = "intervalwarning enabled"
      elif "/FOSHKplugin/intvlwarning=disable" in request_path:
        INTVL_WARNING = False
        logPrint("<INFO> intvlwarning via http/get disabled from " + request_addr)
        htmlout = "intvlwarning disabled"
      elif "/FOSHKplugin/printignored=enable" in request_path:
        BUT_PRINT = True
        logPrint("<INFO> print ignored log entries via http/get enabled from " + request_addr)
        htmlout = "printignored enabled"
      elif "/FOSHKplugin/printignored=disable" in request_path:
        BUT_PRINT = False
        logPrint("<INFO> print ignored log entries via http/get disabled from " + request_addr)
        htmlout = "printignored disabled"
      elif "/FOSHKplugin/loglevel=" in request_path:
        lvl = request_path[22:].upper()
        if lvl in [ "ERROR", "WARNING", "INFO", "ALL" ]:
          global LOG_LEVEL
          LOG_LEVEL = lvl
          logPrint("<INFO> log level set to " + lvl + " via http/get from " + request_addr)
          htmlout = "log level " + lvl + " set"
      elif "/FOSHKplugin/pushover=enable" in request_path:
        if PO_USER != "" and PO_TOKEN != "":
          PO_ENABLE = True
          logPrint("<INFO> pushover warning via http/get enabled from " + request_addr)
          htmlout = "pushover warning enabled"
        else:
          logPrint("<INFO> pushover warning could not be activated from " + request_addr + " - USER or TOKEN are not correctly set in config")
          htmlout = "pushover warning could not be activated - USER or TOKEN are not set in config"
      elif "/FOSHKplugin/pushover=disable" in request_path:
        PO_ENABLE = False
        logPrint("<INFO> pushover warning via http/get disabled from " + request_addr)
        htmlout = "pushover warning disabled"
      # v0.07 - possibility to enable/disable firmware update check via http
      elif "/FOSHKplugin/updatewarning=enable" in request_path:
        if UPD_CHECK:
          checkFW.start()
          logPrint("<INFO> firmware update check with interval " + str(UPD_INTERVAL) + " enabled via http/get from " + request_addr)
          htmlout = "firmware update check enabled (interval: " + str(UPD_INTERVAL) + " sec)"
        else:
          logPrint("<INFO> firmware update check could not be activated from " + request_addr + " - UPD_CHECK or UPD_INTERVAL is not correctly set in config")
          htmlout = "firmware update check could not be activated - UPD_CHECK or UPD_INTERVAL is not correctly set in config"
      elif "/FOSHKplugin/updatewarning=disable" in request_path:
        if UPD_CHECK:
          checkFW.cancel()
          logPrint("<INFO> firmware update check disabled via http/get from " + request_addr)
          htmlout = "firmware update check disabled"
        else:
          logPrint("<INFO> disable firmware update check from " + request_addr + " failed - UPD_CHECK is not set in config")
          htmlout = "disable firmware update check failed - UPD_CHECK is not set in config"
      # 2do: will ich das wirklich?
      elif "/FOSHKplugin/rebootWS" in request_path:
        bootmsg = sendReboot(WS_IP,WS_PORT) if REBOOT_ENABLE else "refused"
        logPrint("<INFO> WS-reboot request via http/get from " + request_addr + " " + bootmsg)
        htmlout = "rebooting weather station " + bootmsg
      elif "/FOSHKplugin/restartPlugin" in request_path:
        restartmsg = killMyself() if RESTART_ENABLE else "refused"
        logPrint("<INFO> FOSHKplugin-restart request via http/get from " + request_addr + " " + restartmsg)
        htmlout = "restarting FOSHKplugin " + restartmsg
      else:                                                    # does not contain fwd-type nor /FOSHKplugin - so just view with body, table aso
        htmlout = "<!DOCTYPE html>\n"
        htmlout += "<html lang=\"en\">\n<head>\n<title>"+prgname+" "+prgver+"</title>\n"
        htmlout += "<meta name=\"viewport\" content=\"width=device-width, initial-scale\=1.0\">\n"
        htmlout += "<link rel=\"icon\" type=\"image/png\" href=\"data:image/png;base64,AAABAAEAEBAAAAEAIABoBAAAFgAAACgAAAAQAAAAIAAAAAEAIAAAAAAAAAQAAMMOAADDDgAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAFAAAABgAAAAYAAAADAAAAA0CbUw9QxGoJUMNpBVDDaRpQw2kOUMNpAlDDaQBQw2kAAAAAAAAAAAUAAAA7AAAAQwAAAFMAAABiAAAAVAAAAF0wdT9YUcZrYlDDaVlQw2lwUMNpZlDDaUZQw2kHUMNpAAAAAAAAAAADAAAAPAAAADwECgVDCRYMTwwdEEQZPiFrK2o5OFLHazlQw2kuUMNpJVDDaTBQw2kkUMNpA1DDaQAAAAAABQUFAE3zcQBRqGQDUcVqZVHFaoJRxmt3UcVqrlDDaYZQw2mXUMNpm1DDaTtQw2kQUMNpAFDDaQAAAAAAAAAAAFDDaQBQw2kAUMNpXVDDab5Qw2mZUMNpIVDDaXlQw2lzUMNpiFDDaX1Qw2lzUMNpfVDDaQ5Qw2kAAAAAAAAAAABQw2kAUMNpAFDDaYBQw2nVUMNpuVDDaRlQw2lxUMNpe1DDaSlQw2kAUMNpAlDDaXJQw2kbUMNpAAAAAAAAAAAAUMNpAFDDaQBQw2l/UMNp1FDDabhQw2kZUMNpclDDaZBQw2mEUMNpaFDDaWxQw2miUMNpH1DDaQAAAAAAAAAAAFDDaQBQw2kAUMNpfVDDadNQw2m1UMNpGFDDaXFQw2liUMNpqFDDaaVQw2moUMNpiVDDaQxQw2kAAAAAAAAAAABQw2kAUMNpAFDDaSlQw2lYUMNpRFDDaQpQw2l1UMNpFlDDaQ5Qw2kPUMNpD1DDaQZQw2kAUMNpAAAAAAAAAAAAAAAAAAAAAABQw2kAUMNpF1DDaX5Qw2lfUMNppVDDaVdQw2mCUMNpKlDDaQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABQw2kAUMNpAFDDaStQw2mzUMNpolDDabFQw2mUUMNpuFDDaUJQw2kAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAUMNpAFDDaSlQw2mIUMNpV1DDaR5Qw2mLUMNpJFDDaUJQw2kzUMNpA1DDaQAAAAAAAAAAAAAAAAAAAAAAAAAAAFDDaQBQw2k4UMNpgFDDaZhQw2mCUMNpl1DDaXpQw2mWUMNpxlDDaSpQw2kAAAAAAAAAAAAAAAAAAAAAAAAAAABQw2kAUMNpN1DDaZ5Qw2lpUMNpElDDaQxQw2kMUMNpIlDDaTRQw2kGUMNpAAAAAAAAAAAAAAAAAAAAAAAAAAAAUMNpAFDDaTBQw2mIUMNpD1DDaQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFDDaQBQw2kGUMNpClDDaQBQw2kAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAEAAIABAACAAQAA4AcAAOADAADgIwAA4AMAAOADAADgBwAA8B8AAPAfAADgDwAA4A8AAOAPAADj/wAA5/8AAA==\">\n"
        htmlout += "<style>table, td, th { vertical-align: top; } table { text-align: left; width: 100%; } button {width:132px;} input {width:290px;}</style>"
        # v0.09: allow http auto refresh via refresh=n
        refreshtime = getURLvalue(request_path,"REFRESH")
        if refreshtime != "":
          try:
            refreshtime = int(refreshtime)
            htmlout += "<meta http-equiv=\"Expires\" content=\"-1\" />\n"
            htmlout += "<meta http-equiv=\"refresh\" content=\""+str(refreshtime)+"\" />\n"
          except: pass
        htmlout += "</head>\n<body>\n"
        if "/FOSHKplugin/getWSconfig" in request_path:
          htmlout += "<table style=\"width:unset;\"><tr><td>ip address:</td><td><input name=\"WS_IP\" id=\"WS_IP\" type=\"text\" value=\"" + WS_IP + "\"/></td><td><button type=\"button\" id=\"getWSIP\">discover</button></td></tr>"
          htmlout += "<tr><td>config port:</td><td><input name=\"WS_PORT\" id=\"WS_PORT\" type=\"text\" value=\"" + WS_PORT + "\"/></td><td><button type=\"button\" id=\"checkPort\">check</button></td></tr>"
          htmlout += "<tr><td>current interval:</td><td><input name=\"WS_INTERVAL\" id=\"WS_INTERVAL\" type=\"text\" value=\"" + WS_INTERVAL + "\"/></td><td><button type=\"button\" id=\"getInterval\">get from ws</button></td></tr>"
          htmlout += "</table>"
          htmlout += "<table style=\"width:unset;\"><tr><td><button type=\"button\" id=\"saveConfig\">save</button></td><td><button type=\"button\" id=\"saveWS\">save to WS</button></td><td><button type=\"button\" id=\"shutdown\">shutdown</button></td><td><button type=\"button\" id=\"restartWS\">restart ws</button></td></tr></table>"
          htmlout += "</body></html>"
        elif "/FOSHKplugin/help" in request_path:
          myIP = socket.gethostbyname(socket.gethostname()) if LB_IP == "" else LB_IP
          myPort = str(LBH_PORT)
          htmlout += "<div style=\"overflow-x:auto;\">"
          htmlout += "<h2>"+prgname+" "+prgver+" help</h2>"
          htmlout += "<h3>Supported URLs and their function:</h3>"
          htmlout += "<h4>program related:</h4>"
          htmlout += "<table><tr><th style=\"width:40%;\">url</th><th style=\"width:20%;\">options</th><th style=\"width:40%;\">comment</th></tr>"
          htmlout += tableRow("/FOSHKplugin/help","","this help screen", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/keyhelp","","shows all available keys known to FOSHKplugin", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/LBU_PORT","","show current UDP command port", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/co2warning=disable","","disable CO2 warning", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/co2warning=enable","","enable CO2 warning", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/debug=disable","","disable debug", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/debug=enable","","enable debug", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/intvlwarning=disable","","disable interval warning", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/intvlwarning=enable","","enable interval warning", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/leakwarning=disable","","disable leak warning", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/leakwarning=enable","","enable leak warning", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/loglevel=ALL","","change logging behaviour, all lines are logged (default)", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/loglevel=ERROR","","change logging behaviour, only lines with ERROR and OK are output", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/loglevel=INFO","","change logging behaviour, all lines except ERROR, WARNING, INFO and OK are hidden (recommended)", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/loglevel=WARNING","","change logging behaviour, all lines except ERROR and WARNING and OK are hidden", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/minmax","","show min/max values", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/patchW4L","","patch W4L (Loxone only)", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/printignored=disable","","disable console output of ignored log-entries", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/printignored=enable","","enable console output of ignored log-entries", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/pushover=disable","","disable pushover notifications (requires PO_ENABLE = True)", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/pushover=enable","","enable pushover notifications (requires PO_ENABLE = True)", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/rebootWS","","reboot weatherstation (requires REBOOT_ENABLE = True)", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/recoverW4L","","unpatch W4L (Loxone only)", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/restartPlugin","","restart FOSHKplugin (requires RESTART_ENABLE = True", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/state","","get running state of FOSHKplugin", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/status","bool","show all statuses", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/updatewarning=disable","","disable firmware update warning", myIP, myPort)
          htmlout += tableRow("/FOSHKplugin/updatewarning=enable","","enable firmware update warning", myIP, myPort)
          htmlout += "</table>"
          htmlout += "<h4>data related:</h4>"
          htmlout += "<table><tr><th style=\"width:40%;\">url</th><th style=\"width:20%;\">options</th><th style=\"width:40%;\">comment</th></tr>"
          htmlout += tableRow("/","separator, units, minmax, status, bool","general overview as webpage", myIP, myPort)
          htmlout += tableRow("/CSVFILE","separator, units, minmax, status, bool","output with key=value as CSV", myIP, myPort)
          htmlout += tableRow("/DATA","separator, units, minmax, status, bool","get data as text", myIP, myPort)
          htmlout += tableRow("/JSON","units, minmax, status, bool","output as as JSON", myIP, myPort)
          htmlout += tableRow("/STRING","separator, units, minmax, status, bool","output as string", myIP, myPort)
          htmlout += tableRow("/UDP","separator, units, minmax, status, bool","last UDP string is output via http", myIP, myPort)
          htmlout += tableRow("/getvalue?key=keyname","bool","output the value only for given keyname; any keyname is allowed", myIP, myPort)
          htmlout += "</table>"
          htmlout += "<h4>output related:</h4>"
          htmlout += "<table><tr><th style=\"width:40%;\">url</th><th style=\"width:20%;\">options</th><th style=\"width:40%;\">comment</th></tr>"
          htmlout += tableRow("/APRS","user","output APRS string; use user=CALLSIGN to exchange DUMMY", myIP, myPort)
          htmlout += tableRow("/CLIENTRAWTXT","","output a clientraw.txt (Weather Display) file", myIP, myPort)
          htmlout += tableRow("/CSV","units","output the values of the last data record as CSV", myIP, myPort)
          htmlout += tableRow("/CSVHDR","separator","output the field names (the header) of the last data record", myIP, myPort)
          htmlout += tableRow("/RAW","separator","output weather station data unchanged via http", myIP, myPort)
          htmlout += tableRow("/REALTIMETXT","","output a realtime.txt (Cumulus) file", myIP, myPort)
          htmlout += tableRow("/SSV","separator, units","output with fixed asignment based on CSV\CSV_FIELDS", myIP, myPort)
          htmlout += tableRow("/SSVHDR","separator","output the field names (the header) as configured in CSV\CSV_FIELDS", myIP, myPort)
          htmlout += tableRow("/WSWIN","","output a WSWin-compatible CSV", myIP, myPort)
          htmlout += tableRow("/observations/current/json/units=e","","WU-compatible data record with imperial values (&deg;F, mph, in, inHg)", myIP, myPort)
          htmlout += tableRow("/observations/current/json/units=m","","WU-compatible data record with metric values (&deg;C, kmh, mm, hPa)", myIP, myPort)
          htmlout += tableRow("/w4l/current.dat","","", myIP, myPort)
          htmlout += "</table>"
          htmlout += "<h4>use options with \"&\" - define separator and units (m for metric and e for imperial) with \"=\"</h4>"
          htmlout += "<table style=\"width:unset;\"><tr><td>&separator=</td><td>&nbsp;</td><td>use to separate fields; any char or string</td></tr>"
          htmlout += "<tr><td>&units=</td><td>&nbsp;</td><td>use m for metric or e for imperial units</td></tr>"
          htmlout += "<tr><td>&minmax</td><td>&nbsp;</td><td>show also the minmax values</td></tr>"
          htmlout += "<tr><td>&status</td><td>&nbsp;</td><td>show also all statuses</td></tr>"
          htmlout += "<tr><td>&bool</td><td>&nbsp;</td><td>show bool-values with true/false instead of 1/0</td></tr></table>"
          htmlout += "<p>example: <a href=\"http://"+myIP+":"+myPort+"/FOSHKplugin/data&separator=%3Cbr%3E&units=m&minmax&status&bool\" target=\"_blank\">http://"+myIP+":"+myPort+"/FOSHKplugin/data&separator=&lt;br&gt;&units=m&minmax&status&bool</a></p>"
          htmlout += "<p>&nbsp;</p>"
          htmlout += "</div>"
          htmlout += "</body></html>"
        elif "/FOSHKplugin/keyhelp" in request_path:
          myLB_IP = socket.gethostbyname(socket.gethostname()) if LB_IP == "" else LB_IP
          htmlout += "<div style=\"overflow-x:auto;\">"
          htmlout += "<h2>"+prgname+" "+prgver+" help</h2>"
          htmlout += "<h3>All known (available) keys:</h3>\n"
          for key in sorted(last_d_all.keys()): htmlout += key+"&nbsp;<a href=\"http://"+myLB_IP+":"+LBH_PORT+"/FOSHKplugin/getvalue?key="+key+"\" target=\"_blank\">(getvalue)</a><br>\n"
          htmlout += "<h3>Statuses:</h3>\n"
          my_d = {}
          my_d = addStatusToDict(my_d, False)
          for key in sorted(my_d.keys()): htmlout += key+"&nbsp;<a href=\"http://"+myLB_IP+":"+LBH_PORT+"/FOSHKplugin/getvalue?key="+key+"\" target=\"_blank\">(getvalue)</a><br>\n"
          htmlout += "<p>&nbsp;</p>"
          htmlout += "</div>"
          htmlout += "</body>\n</html>"
        elif "/FOSHKplugin/showPage" in request_path:
          # 2do - Baustelle
          try:
            with open(CONFIG_DIR+'/foshkplugin.html') as f:
              htmltmp = f.read()
              try:
                htmltmp = convertTemplate(htmltmp)
                htmlout += htmltmp
              except Exception as err:
                htmlout += "Error while injecting variable " + str(err)
                pass
          except: pass
        else:
          htmlout += "<table style=\"width:unset;\">\n<tr><td>"
          last_d_h = last_d_e if "units=e" in request_path else last_d_m
          htmlout += dictToString(last_d_h," ",False,[],[],True,True,True).replace(" ","</td></tr>\n<tr><td>").replace("=","</td><td>")
          htmlout += "</td></tr>\n"
          if "minmax" in request_path:
            #htmlout += "<tr><td>"+dictToString(min_max," ",False,[],["null"],True,True,True).replace(" ","</td></tr>\n<tr><td>").replace("=","</td><td>")
            htmlout += "<tr><td>"+dictToString(metricToImpDict(min_max,[],["-9999","None","null"])," ",False,[],["-9999","None","null"],True,True,True).replace(" ","</td></tr>\n<tr><td>").replace("=","</td><td>") if "units=e" in request_path else "<tr><td>"+dictToString(min_max," ",False,[],["null"],True,True,True).replace(" ","</td></tr>\n<tr><td>").replace("=","</td><td>")
            htmlout += "</td></tr>\n"
          if "status" in request_path:
            sw_what = " (missed: " + SensorIsMissed + ")" if inSensorWarning and SensorIsMissed != "" else ""
            htmlout += "<tr><td>running</td><td>" + str(wsconnected) + "</td></tr>\n<tr><td>wswarning</td><td>" + str(inWStimeoutWarning) + "</td></tr>\n<tr><td>sensorwarning</td><td>" + str(inSensorWarning) + sw_what + "</td></tr>\n<tr><td>batterywarning</td><td>" + str(inBatteryWarning) + "</td></tr>\n<tr><td>stormwarning</td><td>" + str(inStormWarning) + "</td></tr>\n<tr><td>tswarning</td><td>" + str(inTSWarning) + "</td></tr>\n<tr><td>updatewarning</td><td>" + str(updateWarning) + "</td></tr>\n<tr><td>leakwarning</td><td>" + str(inLeakageWarning) + "</td></tr>\n<tr><td>co2warning</td><td>" + str(inCO2Warning) + "</td></tr>\n<tr><td>intvlwarning</td><td>" + str(inIntervalWarning) + "</td></tr>\n<tr><td>time</td><td>" + str(loxTime(time.time())) + "</td></tr>\n"
          htmlout += "</table>\n</body>\n</html>"
          htmlout = htmlout.replace("%20"," ")
      try:
        if refreshtime != "": htmlout += "\n</body>\n"
        self.wfile.write(bytearray(htmlout,OutEncoding))
      except:
        if myDebug: logPrint("<DEBUG> except in wfile.write 2 in do_GET")
        pass
      if str(request_path) != "/favicon.ico":
        logPrint("get-request from " + str(request_addr) + ": " + str(request_path))
    # try to avoid "ConnectionResetError: [Errno 104] Connection reset by peer"
    try:
      self.connection.close()
    except:
      if myDebug: logPrint("<DEBUG> except in close do_GET")
      pass

  def do_POST(self):
    request_path = self.path
    request_addr = self.client_address[0]
    content_length = int(self.headers['content-length'])
    instr = str(self.rfile.read(content_length))
    # check authentication
    if AUTH_PWD != "" and AUTH_PWD not in instr:
      logPrint("<INFO> unauthorized post-request from " + str(request_addr) + ": " + str(request_path))
    elif "report" in request_path:
      # String zusammenbasteln
      instr = instr[2:content_length+2]

      # v0.09: fix GW2000 v2.1.0 firmware bug with ", "
      instr = instr.replace(", runtime=","&runtime=")

      global last_RAWstr
      last_RAWstr = instr

      # v0.09: count real interval time
      global lastData
      global inIntervalWarning
      now = int(time.time())
      if lastData > 0:
        inttime = now - lastData
        intervald.append(inttime)
        l = len(intervald)
        isinterval = int((sum(intervald)-min(intervald)-max(intervald))/(l-2)) if l > 2 else int(sum(intervald)/l)
        #print("isintvl: "+str(inttime)+" l: "+str(l)+" isintvl10: "+str(isinterval)+" all: "+str(intervald))
        if EVAL_VALUES and len(instr) > 0: instr +="&isintvl="+str(inttime)+"&isintvl10="+str(isinterval)
        #print(WS_INTERVAL+" "+str(isinterval)+" "+str(math.ceil(int(WS_INTERVAL) * 1.1))+" "+str(inIntervalWarning))
        if INTVL_WARNING:                                              # warn if measured interval is more than 10% above the agreed send interval
          if isinterval > INTVL_LIMIT and not inIntervalWarning:
            logPrint("<WARNING> real sending interval ("+str(isinterval)+") mismatches the interval set to the weather station ("+str(WS_INTERVAL)+")")
            sendUDP("SID=" + defSID + " intvlwarning=1 time=" + str(loxTime(time.time())))
            pushPrint("<WARNING> real sending interval ("+str(isinterval)+") mismatches the interval set to the weather station ("+str(WS_INTERVAL)+")")
            inIntervalWarning = True
          elif isinterval <= INTVL_LIMIT and inIntervalWarning:        # cancel warning if value is below the warning threshold
            logPrint("<OK> real sending interval ("+str(isinterval)+") matches the interval set to the weather station ("+str(WS_INTERVAL)+") again")
            sendUDP("SID=" + defSID + " intvlwarning=0 time=" + str(loxTime(time.time())))
            pushPrint("<OK> real sending interval ("+str(isinterval)+") matches the interval set to the weather station ("+str(WS_INTERVAL)+") agaim")
            inIntervalWarning = False
      lastData = now

      # ab v0.06: possibly fake the outdoor-sensor with internal values
      if fakeOUT_TEMP != "": instr = instr.replace("&"+fakeOUT_TEMP+"=","&tempf=")
      if fakeOUT_HUM != "": instr = instr.replace("&"+fakeOUT_HUM+"=","&humidity=")

      # v0.07: if configure via Export\OUT_TIME (exchangeTime) replace incoming time string with time of receipt
      if exchangeTime:
        instr = exchangeTimeString(instr)

      # hier ggf. um weitere Felder ergaenzen - etwa dewpt, windchill und feelslike
      #global EVAL_VALUES
      if EVAL_VALUES:
        # erzeugt Wertepaar mit Namen "feld",Wert,Overwrite existent
        instr = addDataToLine(instr,"dewptf",None,False)
        instr = addDataToLine(instr,"windchillf",None,False)
        instr = addDataToLine(instr,"feelslikef",None,False)
        instr = addDataToLine(instr,"heatindexf",None,False)
        instr = addDataToLine(instr,"pm25_AQI",None,False)
        instr = addDataToLine(instr,"windavg",None,False)
        instr = addDataToLine(instr,"brightness",None,False)
        instr = addDataToLine(instr,"cloudf",None,False)
        instr = addDataToLine(instr,"sunhours",None,False)     # combined procedure - dependend on SUN_CALC and existence of lat/lon
        if HIDDEN_FEATURES:                                    # for testing: should be removed in next release
          instr = addDataToLine(instr,"osunhours",None,False)  # old procedure with fixes threshold of 120W/m²
          instr = addDataToLine(instr,"nsunhours",None,False)  # new procedure with dynamic threshold
      if FIX_LIGHTNING and last_lightning_time != 0:
        # set empty keys to last known values
        instr = fixEmptyValue(instr,"lightning_time",str(last_lightning_time))
        instr = fixEmptyValue(instr,"lightning",str(last_lightning))
      # add additional fields (like lat, lon, alt, neighborhood, country or qcStatus)
      global ADD_ITEMS
      if ADD_ITEMS != "":
        if ADD_ITEMS[0] != "&": ADD_ITEMS = "&" + ADD_ITEMS
        instr += ADD_ITEMS

      if rawlog: rawlogger.info(hidePASSKEY(instr))

      # v0.09 possibility to adjust the instr globally - for WS90 compatibility
      # rrain_piezo, erain_piezo, hrain_piezo, drain_piezo, wrain_piezo, mrain_piezo, yrain_piezo
      instr = instrReplace(instr)

      # v0.09 also exchange the keys in last_RAWstr
      last_RAWstr = instrReplace(last_RAWstr)

      # create dictionaries E = Imperial; M = Metric; R = RAW
      d_e = stringToDict(instr,"&")
      d_r = stringToDict(last_RAWstr,"&")
      d_m = convertDictToMetricDict(d_e,IGNORE_EMPTY,LOX_TIME)

      # v0.08 fill the min/max array
      # fill min_max with metric or empire values?
      generateMinMax(d_m)                                      # fill minmax and send via UDP

      global last_d_e
      last_d_e = d_e
      global last_d_m
      last_d_m = d_m

      # v0.09 one global list for all (except status) lists
      global last_d_all
      last_d_all = last_d_e.copy()
      last_d_all.update(last_d_m)
      last_d_all.update(min_max)
      last_d_all.update(metricToImpDict(min_max,[],["null"]))

      # v0.08 add ptrend1, pchange1, ptrend3 & pchange3 - needs d_m and is for instr only
      if EVAL_VALUES:
        instr = addDataToLine(instr,"ptrend",None,False)

      # zerlegen
      UDPstr = "SID=" + defSID + " " + dictToString(d_m," ",True,UDP_IGNORE) if USE_METRIC else "SID=" + defSID + " " + dictToString(d_e," ",True,UDP_IGNORE)

      # jetzt UDPstr versenden
      sendUDP(UDPstr)

      # for GW1000/DP1500 no response needed; but in forward-mode (myself) this is a must
      #OKanswer = "OK\n"
      OKanswer = EWpostOKstr()+"\n"
      try:
        self.send_response(200)
        self.send_header('Content-Type','text/html')
        self.send_header('Content-Length',str(len(OKanswer)))
        self.send_header('Connection','Close')
        self.end_headers()
      except:
        if myDebug: logPrint("<DEBUG> except in header-response in do_POST")
        pass
      # 2do: v0.07 always reply OK to satisfy the Ecowitt-watchdog - but perhaps they need just 0x0A or anything
      try:
        self.wfile.write(bytearray(OKanswer,OutEncoding))
      except:
        if myDebug: logPrint("<DEBUG> except in wfile.write in do_POST")
        pass
      global last_csv_time
      # letzte Meldung der Wetterstation merken
      global last_ws_time
      last_ws_time = int(time.time())
      # String nach WU-String umwandeln und an alle FWD_URL im gesetzen Intervall versenden
      if forwardMode:
        for i in range(len(fwd_arr)):                          # 0:url,1:interval,2:interval_num,3:last,4:ignore,5:type,6:fwd_sid,7:fwd_pwd,8:status,9:minmax,10:script,11:nr,12:mqttcycle,13:fwd_remap
          if time.time() >= fwd_arr[i][3]+fwd_arr[i][2]:
            if fwd_arr[i][5] == "WU":                          # String nach WU wandeln und per get versenden
              t = threading.Thread(target=forwardStringToWU, args=(fwd_arr[i][0],instr,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "RAW":                       # RAW-Dict ohne Aenderung per post weitersenden
              t = threading.Thread(target=forwardDictToHTTP, args=(fwd_arr[i][0],d_r,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],True,True,False,"&"))
              t.start()
            elif fwd_arr[i][5] == "EW":                        # eingehenden, erweiterten RAW-String nach Ecowitt wandeln und per post versenden
              t = threading.Thread(target=forwardStringToEW, args=(fwd_arr[i][0],instr,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] in ("RAWEW","EWRAW"):           # eingehenden RAW-String nach Ecowitt wandeln und per post versenden
              t = threading.Thread(target=forwardStringToEW, args=(fwd_arr[i][0],last_RAWstr,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "LD":                        # forward pm25 value only to luftdaten.info; args: url, fwd_sid, wert
              t = threading.Thread(target=forwardDictToLuftdaten, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],))
              t.start()
            elif fwd_arr[i][5] == "UDP":                       # forward metr. or imp. dict per UDP (other target than Loxone)
              d_fwd = d_m if USE_METRIC else d_e
              t = threading.Thread(target=forwardDictToUDP, args=(fwd_arr[i][0],d_fwd,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]," "))
              t.start()
            elif fwd_arr[i][5] in ("RAWUDP","UDPRAW"):         # forward incoming string via UDP
              t = threading.Thread(target=forwardStringToUDP, args=(fwd_arr[i][0],instr,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] in ("EWUDP","UDPEW"):           # forward imp. dict per UDP (convert to EW-format)
              t = threading.Thread(target=forwardDictToUDP, args=(fwd_arr[i][0],d_e,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],"&"))
              t.start()
            elif fwd_arr[i][5] in ("RAWCSV","CSVRAW"):         # forward the raw values as CSV-string for e.g. Edomi
              t = threading.Thread(target=forwardDictToHTTP, args=(fwd_arr[i][0],d_r,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],True,True,False,";"))
              t.start()
            elif fwd_arr[i][5] == "CSV":                       # forward as CSV-string for e.g. Edomi
              d_fwd = d_m if USE_METRIC else d_e
              t = threading.Thread(target=forwardDictToHTTP, args=(fwd_arr[i][0],d_fwd,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],True,True,False,";"))
              t.start()
            elif fwd_arr[i][5] == "AMB":                       # convert incoming string to Ambient and send via GET
              t = threading.Thread(target=forwardStringToAMB, args=(fwd_arr[i][0],instr,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] in ("RAWAMB","AMBRAW"):         # convert incoming RAW-string to Ambient and send via GET
              t = threading.Thread(target=forwardStringToAMB, args=(fwd_arr[i][0],last_RAWstr,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "MT":                        # convert metric dict to Meteotemplate and send via GET
              t = threading.Thread(target=forwardDictToMeteoTemplate, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "WC":                        # convert metric dict to WeatherCloud and send via GET
              t = threading.Thread(target=forwardDictToWC, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "AWEKAS":                    # convert metric dict to Awekas-API and send via GET
              t = threading.Thread(target=forwardDictToAwekas, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "WETTERCOM":                 # convert metric dict to wetter.com-API and send via GET
              t = threading.Thread(target=forwardDictToWetterCOM, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "WEATHER365":                # convert metric dict to weather365-API and send via POST
              t = threading.Thread(target=forwardDictToWeather365, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "WETTERSEKTOR":              # convert metric dict to Wettersektor-API via POST
              t = threading.Thread(target=forwardDictToWetterSektor, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            elif fwd_arr[i][5] == "MQTTMET":                   # send metric dict to MQTT server
              t = threading.Thread(target=forwardDictToMQTT, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],fwd_arr[i][12],True))
              t.start()
            elif fwd_arr[i][5] == "MQTTIMP":                   # send imperial dict to MQTT server
              t = threading.Thread(target=forwardDictToMQTT, args=(fwd_arr[i][0],d_e,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],fwd_arr[i][12],False))
              t.start()
            elif fwd_arr[i][5] == "INFLUXMET":                 # send metric dict to InfluxDB server
              t = threading.Thread(target=forwardDictToInfluxDB, args=(fwd_arr[i][0],d_m,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],True))
              t.start()
            elif fwd_arr[i][5] == "INFLUXIMP":                 # send imperial dict to InfluxDB server
              t = threading.Thread(target=forwardDictToInfluxDB, args=(fwd_arr[i][0],d_e,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],False))
              t.start()
            elif fwd_arr[i][5] in ("REALTIMETXT","CLIENTRAWTXT","CSVFILE","TXTFILE","TEXTFILE","RAWTEXT","WSWIN"):      # convert dict to file
              d_fwd = d_e if fwd_arr[i][5] == "RAWTEXT" else d_m                                                        # use imperial dict for RAWTEXT only
              t = threading.Thread(target=forwardDictToFile, args=(fwd_arr[i][0],d_fwd,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],fwd_arr[i][5]))
              t.start()
            elif fwd_arr[i][5] == "APRS":                      # convert imperial dict to APRS and send via TCP/IP
              t = threading.Thread(target=forwardDictToAPRS, args=(fwd_arr[i][0],d_e,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13]))
              t.start()
            else:                                              # metr. oder imperiales dict wie UDP-String per get versenden
              d_fwd = d_m if USE_METRIC else d_e
              t = threading.Thread(target=forwardDictToHTTP, args=(fwd_arr[i][0],d_fwd,fwd_arr[i][6],fwd_arr[i][7],fwd_arr[i][8],fwd_arr[i][10],fwd_arr[i][11],fwd_arr[i][4],fwd_arr[i][13],False,True,True,"&"))
              t.start()
            fwd_arr[i][3] = time.time()
      if CSVsave and time.time() >= last_csv_time + CSV_INTERVAL_num:
        if last_csv_time == 0:
          hname = "/tmp/"+prgname+"-"+LBH_PORT+".csvheader"
          try:
            hfile = open(hname,"w+")
            d_fwd = d_m if USE_METRIC else d_e
            hfile.write(dictToString(d_fwd,";",True,[],[],True,False))
            hfile.close()
            logPrint("<OK> CSV-header-file " + hname + " written")
          except:
            logPrint("<ERROR> unable to write CSV-header-file to " + hname + "!")
            pass
        csvline = lineToCSV(d_m,CSV_FIELDS) if USE_METRIC else lineToCSV(d_e,CSV_FIELDS)
        try:
          fcsv.write(csvline + "\r\n")
          fcsv.flush()
        except:
          sndPrint("<ERROR> unable to write the record to " + CSV_NAME + "!",True)
          pass
        if sndlog: sndPrint("CSV: " + csvline)
        last_csv_time = time.time()
    else:
      logPrint("post-request from " + str(request_addr) + ": " + str(request_path))
    # try to avoid "ConnectionResetError: [Errno 104] Connection reset by peer"
    try:
      self.connection.close()
    except:
      if myDebug: logPrint("<DEBUG> except in close do_POST")
      pass

  def log_message(self, format, *args):
    return

  try:
    do_PUT = do_POST
  except:
    if myDebug: logPrint("<DEBUG> except in outer do_POST")
    pass
  try:
    do_DELETE = do_GET
  except:
    if myDebug: logPrint("<DEBUG> except in outer do_GET")
    pass

def getWSconfig(what = "") :
  tries = 5                                      # Anzahl der Versuche
  v = 0
  wsCONFIG = "not found - try again!"
  while wsCONFIG == "not found - try again!" and v <= tries:
    # Set up UDP socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.settimeout(udpTimeOut)
    s.bind(('',43210))
    s.sendto(bytearray(cmd_discover,'latin-1'), ('255.255.255.255', 46000) )
    found = False
    try:
      while not found :
        data, addr = s.recvfrom(11200)
        # gibt es eine korrekte Rueckgabe?
        if len(data) > 15:
          if what == "IP":
            wsCONFIG = str(data[11]) + "." + str(data[12]) + "." + str(data[13]) + "." + str(data[14])
            found = True
          elif what == "PORT":
            wsCONFIG = str(data[15]*256 + data[16])
            found = True
      s.close()
    except socket.error:
      pass
    v +=1
  #print(wsCONFIG + " Versuche: " + str(v))
  return wsCONFIG

def scanWS() :
  devices = []
  # Set up UDP socket
  s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
  s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
  s.settimeout(udpTimeOut)  # was 2
  s.sendto(bytearray(cmd_discover,'latin-1'), ('255.255.255.255', 46000) )
  try:
    while True:
      data, addr = s.recvfrom(11200)
      if len(data) > 14:
        mac = ""
        for i in range(5,11):
          if data[i] < 10: mac += "0"
          mac += str(hex(data[i]))+":"
        mac = mac.replace("0x","").upper()[:-1]
        ip = str(data[11]) + "." + str(data[12]) + "." + str(data[13]) + "." + str(data[14])
        port = data[15]*0x100 + data[16]
        name = ""
        for i in range(18,18+data[17]):
          name += chr(data[i])
        devices.append([mac,ip,str(port),name])
  except socket.timeout:
    pass
  s.close()
  l = len(devices)
  print()
  if l == 0:
    print("no device found!")
  elif l == 1:
    print("1 device found:")
  elif l > 1:
    print("more than 1 device found:")
  if l > 0:
    for i in range(0,len(devices)):
      # 0=mac 1=ip 2=port 3=name
      print("ip"+str(i)+": " + devices[i][1]+" name: "+devices[i][3]+" port: "+devices[i][2]+" mac: "+devices[i][0])
  print()

def sendToWS(ws_ipaddr, ws_port, cmd):           # oeffnet jeweils einen neuen Socket und verschickt cmd; Rueckmeldung = Rueckmeldung der WS
  tries = 5                                      # Anzahl der Versuche
  v = 0
  data = ""
  while data == "" and v <= tries:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.settimeout(udpTimeOut)
    try:
      s.connect((ws_ipaddr, int(ws_port)))
      s.sendall(cmd)
      data, addr = s.recvfrom(11200)
      s.close()
    except:
      pass
    v +=1
  return data

def sendReboot(ws_ipaddr, ws_port):
  #answer = sendToWS(ws_ipaddr, ws_port, bytearray(cmd_reboot,'latin-1'))
  #ret = "done" if answer == bytearray(ok_cmd_reboot,'latin-1') else "failed"
  #return ret
  return "done" if sendToWS(ws_ipaddr, ws_port, bytearray(cmd_reboot,'latin-1')) == bytearray(ok_cmd_reboot,'latin-1') else "failed"

def crcsum(data):
  summe=0
  for i in range(2,len(data)-1): summe = summe + data[i]
  return summe % 256

def byteTohex(b):
  z = str(hex(b))
  s = z[:2]+"0"+z[2:] + " " if len(z)<4 else z + " "
  return s

def arrTohexOrig(a):
  s = ""
  for i in range(len(a)):
    z = str(hex(a[i]))
    s += z[:2]+"0"+z[2:] + " " if len(z)<4 else z + " "
  return s

def arrTohex(a):
  s = ""
  for i in range(len(a)):
    s += byteTohex(a[i])
  return s

def setWSconfig(ws_ipaddr, ws_port, custom_host, custom_port, custom_interval):
  # aktuelle Config auslesen, mit den Parametern ersetzen und in WS schreiben
  didNotWork = True
  mod_cdata = ""
  mod_edata = ""
  # customC abfragen und als orig_cdata merken
  cdata = sendToWS(ws_ipaddr, ws_port, bytearray(cmd_get_customC,'latin-1'))
  orig_cdata = cdata
  # customE abfragen und als orig_edata merken
  edata = sendToWS(ws_ipaddr, ws_port, bytearray(cmd_get_customE,'latin-1'))
  orig_edata = edata
  # Variablen fuellen
  if (cdata != "" and len(cdata) >= 6) and (edata != "" and len(edata) >= 12):
    pe_len = cdata[4]
    pw_len = cdata[pe_len + 5]
    ws_custom_ecpath = ""
    ws_custom_wupath = ""
    for i in range(5,5 + pe_len): ws_custom_ecpath += chr(cdata[i])
    for i in range(pe_len + 6,pe_len + 6 + pw_len): ws_custom_wupath += chr(cdata[i])

    id_len = edata[4]
    key_len = edata[id_len + 5]
    ip_len = edata[key_len + id_len + 6]
    ws_custom_id = ""
    ws_custom_key = ""
    ws_custom_host = ""
    for i in range(5,5 + id_len): ws_custom_id += chr(edata[i])
    for i in range(id_len + 6,id_len + 6 + key_len): ws_custom_key += chr(edata[i])
    for i in range(key_len + id_len + 7,key_len + id_len + 7 + ip_len): ws_custom_host += chr(edata[i])
    ws_custom_port = edata[ip_len + key_len + id_len + 7]*256 + edata[ip_len + key_len + id_len + 8]
    ws_custom_interval = edata[ip_len + key_len + id_len + 9]*256 + edata[ip_len + key_len + id_len + 10]
    ws_custom_ecowitt = not bool(edata[ip_len + key_len + id_len + 11])
    ws_custom_enabled = bool(edata[ip_len + key_len + id_len + 12])

    # jetzt Werte austauschen und mit neuen Werten in WS schreiben
    ws_custom_enabled = True
    ws_custom_ecowitt = True
    # leere ID & leeren Key verhindern - nicht (mehr) noetig!
    #if ws_custom_id == "": ws_custom_id = "id"
    #if ws_custom_key == "": ws_custom_key = "key"

    # falls es nur um das Schreiben des Intervalls geht
    if custom_host == "-" and custom_port == "-":
      custom_host = ws_custom_host
      custom_port = ws_custom_port
    else:
      # Path generell neu schreiben - stellt korrekten Path sicher
      ws_custom_ecpath = "/data/report/"
      ws_custom_wupath = "/weatherstation/updateweatherstation.php?"

    # Werte schreiben
    cmd = cmd_set_customC + " " + chr(len(ws_custom_ecpath)) + ws_custom_ecpath + chr(len(ws_custom_wupath)) + ws_custom_wupath + "\x00"
    arr = bytearray(cmd,'latin-1')
    # adjust len-Byte in command - do not count header (FFFF) - have to be done before crcsum!
    arr[3] = len(arr)-2
    arr[len(arr)-1] = crcsum(arr)
    mod_cdata = arr
    cdata = sendToWS(ws_ipaddr, ws_port, arr)

    cmd = cmd_set_customE + " " + chr(len(ws_custom_id)) + ws_custom_id + chr(len(ws_custom_key)) + ws_custom_key + chr(len(custom_host)) + custom_host + chr(int(int(custom_port)/256)) + chr(int(int(custom_port)%256)) + chr(int(int(custom_interval)/256)) + chr(int(int(custom_interval)%256)) + chr(not ws_custom_ecowitt) + chr(ws_custom_enabled) + "\x00"
    arr = bytearray(cmd,'latin-1')
    # adjust len-Byte in command - do not count header (FFFF) - have to be done before crcsum!
    arr[3] = len(arr)-2
    arr[len(arr)-1] = crcsum(arr)
    mod_edata = arr
    edata = sendToWS(ws_ipaddr, ws_port, arr)

    # Rueckgabewerte pruefen - bei Misserfolg Daten ins Log
    if cdata == bytearray(ok_set_customC,'latin-1') and edata == bytearray(ok_set_customE,'latin-1') :
      outstr = "<OK> enable custom server on WS " + str(ws_ipaddr) + ":" + str(ws_port) + "; sending to " + str(custom_host) + ":" + str(custom_port) + " in Ecowitt every " + str(custom_interval) + "sec: ok"
      didNotWork = False
    else:
      outstr = "<ERROR> enable custom server on WS " + str(ws_ipaddr) + ":" + str(ws_port) + "; sending to " + str(custom_host) + ":" + str(custom_port) + " in Ecowitt every " + str(custom_interval) + "sec: failed"
  else:
    outstr = "<ERROR> error while reading current configuration of weather station " + ws_ipaddr + " on port " + ws_port
  if didNotWork:
    # cdata und edata enthalten die Rueckgabewerte von der Wetterstation
    # orig_cdata und orig_edata enthalten die urspruenglichen Werte der Wetterstation
    # mod_cdata und mod_edata enthalten die durch das Plugin veraenderten Werte
    logPrint("<ERROR> original cdata:  " + arrTohex(orig_cdata))
    logPrint("<ERROR> modified cdata:  " + arrTohex(mod_cdata))
    logPrint("<ERROR> result of cdata: " + arrTohex(cdata))
    logPrint("<ERROR> original edata:  " + arrTohex(orig_edata))
    logPrint("<ERROR> modified edata:  " + arrTohex(mod_edata))
    logPrint("<ERROR> result of edata: " + arrTohex(edata))
  elif myDebug:
    logPrint("<DEBUG> original cdata:  " + arrTohex(orig_cdata))
    logPrint("<DEBUG> modified cdata:  " + arrTohex(mod_cdata))
    logPrint("<DEBUG> result of cdata: " + arrTohex(cdata))
    logPrint("<DEBUG> original edata:  " + arrTohex(orig_edata))
    logPrint("<DEBUG> modified edata:  " + arrTohex(mod_edata))
    logPrint("<DEBUG> result of edata: " + arrTohex(edata))
  return outstr

def getWSINTERVAL(ws_ipaddr, ws_port) :
  edata = sendToWS(ws_ipaddr, ws_port, bytearray(cmd_get_customE,'latin-1'))
  if edata != "" and len(edata) >= 12:
    id_len = edata[4]
    key_len = edata[id_len + 5]
    ip_len = edata[key_len + id_len + 6]
    wsINTERVAL = str(edata[ip_len + key_len + id_len + 9]*256 + edata[ip_len + key_len + id_len + 10])
  else:
    wsINTERVAL = "not found - try again!"
  #print("ip: " + ws_ipaddr + " port: " + ws_port + " " + " Versuche: " + str(v))
  #print("edata: " + str(arrTohex(edata)))
  return wsINTERVAL

#formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s %(message)s',datefmt="%d.%m.%Y %H:%M:%S")
formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(message)s',datefmt="%d.%m.%Y %H:%M:%S")

def setup_logger(name, log_file, level=logging.INFO, format=formatter):
  handler = logging.handlers.WatchedFileHandler(log_file)
  handler.setFormatter(format)
  logger = logging.getLogger(name)
  logger.setLevel(level)
  logger.addHandler(handler)
  return logger

def checkLBPort(IP,PORT,proto):
  udpopen = False
  #print("IP: " + IP + " PORT: " + str(PORT) + " " + str(udpopen))
  if proto == "UDP":
    ssock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
  else:
    ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Internet, TCP
  try:
    ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ssock.bind((IP, int(PORT)))
    ssock.close()
    #print("port is usable")
    udpopen = True
  except OSError as msg:
    #print('could not open socket')
    pass
  return udpopen

def savePickle(CONFIG_FILE, fname):
  # save current time in Config-File
  try:
    config = readConfigFile(CONFIG_FILE)
    if not config.has_section("Status"): config.add_section('Status')
    stoptime = str(int(time.time()))
    config.set("Status","StopTime",stoptime)
    with open(CONFIG_FILE, "w") as configfile: config.write(configfile)
    if myDebug: logPrint("<DEBUG> savePickle: StopTime set to " + stoptime + " in " + CONFIG_FILE)
  except:
    if myDebug: logPrint("<ERROR> savePickle: can not write StopTime " + stoptime + " to " + CONFIG_FILE)
    pass
  # save pickle
  anz_stundenwerte = len(stundenwerte)
  #logPrint("<DEBUG> anz_stundenwerte: "+str(anz_stundenwerte))
  # v0.08 write only when necessary
  if anz_stundenwerte > 0:
    if myDebug: logPrint("<DEBUG> savePickle: write stundenwerte to " + fname)
    try:
      with open(fname, "wb") as output:
        try:
          pickle.dump(stundenwerte, output, pickle.HIGHEST_PROTOCOL)
          logPrint("<OK> wrote stundenwerte to " + fname + " (" + str(anz_stundenwerte) + ")")
        except:
          logPrint("<ERROR> unable to write stundenwerte to " + fname)
          pass
    except OSError as e:
      logPrint("<ERROR> unable to write stundenwerte to " + fname + ": " + str(e))
      pass

def terminateProcess(signalNumber, frame):
  #if STORM_WARNING: savePickle(CONFIG_FILE, CONFIG_DIR+"/"+prgname+"-"+LBH_PORT+"-stundenwerte.pkl")
  #sendUDP("SID=" + defSID + " running=0")
  #allPrint("<OK> "+prgname+" "+prgver+" stopped")
  if myDebug: logPrint("<DEBUG> terminateProcess through signal " + str(signalNumber))
  # vielleicht reicht auch schon da Setzen von wsconnected = False?
  # nein - aus unerfindlichen Gruenden muss sys.exit() erfolgen!
  global wsconnected
  wsconnected = False
  sys.exit()

class InfiniteTimer():
  """A Timer class that does not stop, unless you want it to."""

  def __init__(self, seconds, target):
    self._should_continue = False
    self.is_running = False
    self.seconds = seconds
    self.target = target
    self.thread = None

  def _handle_target(self):
    self.is_running = True
    self.target()
    self.is_running = False
    self._start_timer()

  def _start_timer(self):
    if self._should_continue: # Code could have been running when cancel was called.
      self.thread = Timer(self.seconds, self._handle_target)
      self.thread.start()

  def start(self):
    if not self._should_continue and not self.is_running:
      self._should_continue = True
      self._start_timer()
    else:
      print("Timer already started or running, please wait if you're restarting.")

  def cancel(self):
    if self.thread is not None:
      self._should_continue = False # Just in case thread is running and cancel fails.
      self.thread.cancel()
    else:
      print("Timer never started or failed to initialize.")

def checkWS_report():
  #print("checkWS: " + str(int(time.time())) + " int: " + WS_INTERVAL + " last: " + str(last_ws_time) + " now: " + str(int(time.time())))
  global inWStimeoutWarning
  global CONFIG_FILE
  if last_ws_time > 0:
    if time.time() > last_ws_time + WSDOG_INTERVAL * int(WS_INTERVAL):
      if not inWStimeoutWarning:
        logPrint("<WARNING> weather station has not reported data for more than " + str(WSDOG_INTERVAL*int(WS_INTERVAL)) + " seconds (" + str(WSDOG_INTERVAL) + " send-intervals)")
        sendUDP("SID=" + defSID + " wswarning=1 last=" + str(loxTime(last_ws_time)) + " time="  + str(loxTime(time.time())))
        pushPrint("<WARNING> weather station has not reported data for more than " + str(WSDOG_INTERVAL*int(WS_INTERVAL)) + " seconds (" + str(WSDOG_INTERVAL) + " send-intervals)")
        inWStimeoutWarning = True
        # save status in Config-file
        config = readConfigFile(CONFIG_FILE)
        if not config.has_section("Status"): config.add_section('Status')
        config.set("Status","inWStimeoutWarning",str(inWStimeoutWarning))
        with open(CONFIG_FILE, "w") as configfile: config.write(configfile)
      elif WSDOG_RESTART > 0 and time.time() > last_ws_time + WSDOG_RESTART * int(WS_INTERVAL):
        logPrint("<WARNING> weather station has not reported data for more than " + str(WSDOG_RESTART*int(WS_INTERVAL)) + " seconds (" + str(WSDOG_RESTART) + " send-intervals) - restarting " + prgname)
        pushPrint("<WARNING> weather station has not reported data for more than " + str(WSDOG_RESTART*int(WS_INTERVAL)) + " seconds (" + str(WSDOG_RESTART) + " send-intervals) - restarting " + prgname)
        global wsconnected
        wsconnected = False
        killMyself()
        if myDebug: logPrint("<DEBUG> restart via UDP done")
    elif inWStimeoutWarning:
      logPrint("<OK> weather station has reported data again")
      sendUDP("SID=" + defSID + " wswarning=0 last=" + str(loxTime(last_ws_time)) + " time="  + str(loxTime(time.time())))
      pushPrint("<OK> weather station has reported data again")
      # clean up status in Config-file
      config = readConfigFile(CONFIG_FILE)
      config.remove_option("Status","inWStimeoutWarning")
      with open(CONFIG_FILE, "w") as configfile: config.write(configfile)
      inWStimeoutWarning = False
    # v0.08 send warnings via UDP on regular basis
    global UDP_STATRESEND_time
    if UDP_STATRESEND > 0 and time.time() >= UDP_STATRESEND_time + UDP_STATRESEND:
      sw_what = " missed=" + SensorIsMissed if inSensorWarning and SensorIsMissed != "" else ""
      statestr = "SID=" + defSID + " running=" + str(int(wsconnected)) + " wswarning=" + str(int(inWStimeoutWarning)) +  " sensorwarning=" + str(int(inSensorWarning)) + sw_what + " batterywarning=" + str(int(inBatteryWarning)) + " stormwarning=" + str(int(inStormWarning)) + " tswarning=" + str(int(inTSWarning)) + " updatewarning=" + str(int(updateWarning)) + " leakwarning=" + str(int(inLeakageWarning)) + " co2warning=" + str(int(inCO2Warning)) + " intvlwarning=" + str(int(inIntervalWarning))
      #print(statestr)
      sendUDP(statestr)
      UDP_STATRESEND_time = time.time()

def thisDay(when):                                             # check if saved day (when) is same as current day
  return True if time.strftime("%Y-%m-%d",time.localtime(when)) == time.strftime("%Y-%m-%d",time.localtime(int(time.time()))) else False

def initMinMax():                                              # create and initialize an empty min/max array
  global min_max
  min_max = { "minmax_init" : int(time.time()) }               # last init time in localtime
  min_max.update({"baromrelhpa_min" : "null", "baromrelhpa_min_time" : "null", "baromrelhpa_max" : "null", "baromrelhpa_max_time" : "null"})
  min_max.update({"humidity_min" : "null", "humidity_min_time" : "null", "humidity_max" : "null", "humidity_max_time" : "null"})
  min_max.update({"tempc_min" : "null", "tempc_min_time" : "null", "tempc_max" : "null", "tempc_max_time" : "null"})
  min_max.update({"windchillc_min" : "null", "windchillc_min_time" : "null", "windchillc_max" : "null", "windchillc_max_time" : "null"})
  min_max.update({"heatindexc_min" : "null", "heatindexc_min_time" : "null", "heatindexc_max" : "null", "heatindexc_max_time" : "null"})
  min_max.update({"feelslikec_min" : "null", "feelslikec_min_time" : "null", "feelslikec_max" : "null", "feelslikec_max_time" : "null"})
  min_max.update({"dewptc_min" : "null", "dewptc_min_time" : "null", "dewptc_max" : "null", "dewptc_max_time" : "null"})
  min_max.update({"tempinc_min" : "null", "tempinc_min_time" : "null", "tempinc_max" : "null", "tempinc_max_time" : "null"})
  min_max.update({"humidityin_min" : "null", "humidityin_min_time" : "null", "humidityin_max" : "null", "humidityin_max_time" : "null"})
  for i in range(1,9):
    min_max.update({"temp"+str(i)+"c_min" : "null", "temp"+str(i)+"c_min_time" : "null", "temp"+str(i)+"c_max" : "null", "temp"+str(i)+"c_max_time" : "null"})
    min_max.update({"humidity"+str(i)+"_min" : "null", "humidity"+str(i)+"_min_time" : "null", "humidity"+str(i)+"_max" : "null", "humidity"+str(i)+"_max_time" : "null"})
  # WH45 temp/hum
  min_max.update({"tc_co2_min" : "null", "tc_co2_min_time" : "null", "tc_co2_max" : "null", "tc_co2_max_time" : "null"})
  min_max.update({"humi_co2_min" : "null", "humi_co2_min_time" : "null", "humi_co2_max" : "null", "humi_co2_max_time" : "null"})
  for i in range(1,9):
    min_max.update({"tf_ch"+str(i)+"c_min" : "null", "tf_ch"+str(i)+"c_min_time" : "null", "tf_ch"+str(i)+"c_max" : "null", "tf_ch"+str(i)+"c_max_time" : "null"})
  min_max.update({"windspeedkmh_max" : "null", "windspeedkmh_max_time" : "null"})
  min_max.update({"windgustkmh_max" : "null", "windgustkmh_max_time" : "null"})
  min_max.update({"solarradiation_min" : "null", "solarradiation_min_time" : "null", "solarradiation_max" : "null", "solarradiation_max_time" : "null"})
  min_max.update({"uv_min" : "null", "uv_min_time" : "null", "uv_max" : "null", "uv_max_time" : "null"})
  min_max.update({"sunmins" : 0})                              # combined: count of minutes with SR >= 120 or dynamic threshold
  min_max.update({"last_suntime" : ""})                        # combined: time of last sun data reception
  min_max.update({"last_suncheck" : ""})                       # combined: time of last check of sun data reception
  if HIDDEN_FEATURES:
    min_max.update({"osunmins" : 0})                           # old: count of minutes with SR >= 120
    min_max.update({"last_osuntime" : ""})                     # old: time of last sun data reception
    min_max.update({"last_osuncheck" : ""})                    # old: time of last check of sun data reception
    min_max.update({"nsunmins" : 0})                           # new: count of minutes with SR >= dynamic threshold
    min_max.update({"last_nsuntime" : ""})                     # new: time of last real sun data reception (sr > SUN_MIN & real sun recognition)
    min_max.update({"last_nsuncheck" : ""})                    # new: time of last sun data reception (sr > SUN_MIN)
  min_max.update({"rainratemm_min" : "null", "rainratemm_min_time" : "null", "rainratemm_max" : "null", "rainratemm_max_time" : "null"})
  min_max.update({"dailyrainmm_min" : "null", "dailyrainmm_min_time" : "null", "dailyrainmm_max" : "null", "dailyrainmm_max_time" : "null"})
  what = "soilmoisture"
  for i in range(1,9):
    min_max.update({what+str(i)+"_min" : "null", what+str(i)+"_min_time" : "null", what+str(i)+"_max" : "null", what+str(i)+"_max_time" : "null"})
  for i in range(1,9):
    min_max.update({"leafwetness_ch"+str(i)+"_min" : "null", "leafwetness_ch"+str(i)+"_min_time" : "null", "leafwetness_ch"+str(i)+"_max" : "null", "leafwetness_ch"+str(i)+"_max_time" : "null"})
  #min_max.update({"humidex_min" : "null", "humidex_min_time" : "null", "humidex_max" : "null", "humidex_max_time" : "null"})

def calcMinMax(value, what, is_time):                          # set min/max for given keys as string
  global min_max
  outstr = ""
  try:
    if value != "null" and (min_max[what+"_min"] == "null" or float(value) < float(min_max[what+"_min"])):
      min_max[what+"_min"] = value
      min_max[what+"_min_time"] = is_time
      if what+"_min" not in UDP_IGNORE:
        outstr += what+"_min="+value + " " + what+"_min_time="+str(loxTime(is_time))+" "
  except (KeyError, ValueError, TypeError):
    pass
  try:
    if value != "null" and (min_max[what+"_max"] == "null" or float(value) > float(min_max[what+"_max"])):
      min_max[what+"_max"] = value
      min_max[what+"_max_time"] = is_time
      if what+"_max" not in UDP_IGNORE:
        outstr += what+"_max="+value + " " + what+"_max_time="+str(loxTime(is_time))+" "
  except (KeyError, ValueError, TypeError):
    pass
  return outstr

def saveMinMax(fname):                                         # save the current min/max array to file
  try:
    with open(fname, "wb") as output:
      try:
        pickle.dump(min_max, output, pickle.HIGHEST_PROTOCOL)
        logPrint("<OK> wrote min/max values to " + fname + " (" + str(len(min_max)) + ")")
      except:
        logPrint("<ERROR> unable to write min/max values to " + fname)
        pass
  except OSError as e:
    logPrint("<ERROR> unable to write min/max values to " + fname + ": " + str(e))
    pass

def loadMinMax(fname):                                         # load the min/max array from file
  global min_max
  if os.path.exists(fname):
    with open(fname, 'rb') as input:
      try:
        min_max = pickle.load(input)
        logPrint("<OK> loaded min/max values from " + fname + " (" + str(len(min_max)) + ")")
      except:
        initMinMax()
        logPrint("<WARNING> unable to load min/max values from " + fname)
        pass
  else:
    initMinMax()

def tstampstrToZeit(where, localtime=True):                    # convert timestamp to time str
  outstr = ""
  try:
    outstr = time.strftime("%H:%M:%S", time.localtime(int(where))) if localtime else time.strftime("%H:%M:%S", time.gmtime(int(where)))
  except: pass
  return outstr

def minmaxCSVline():                                           # create CSV line with min/max values
  mmstr = ""
  for key, value in min_max.items():
    if "minmax_init" in key or "time" in key:
      mmstr += tstampstrToZeit(getfromDict(min_max,[key])) + ";"
    elif value == "null":
      mmstr += ";"
    else:
      mmstr += str(value).replace(".",",") + ";"
  if len(mmstr) > 0 and mmstr[-1] == ";": mmstr = mmstr[:-1]
  mmstr = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime(time.time())) + ";" + mmstr
  return mmstr

def moreFields(hdr_arr,src_arr,sep=";",header=False):
  outstr = ""
  for i in range(len(hdr_arr)): 
    if header: outstr += hdr_arr[i]+sep
    else:
      value = getfromDict(src_arr,[hdr_arr[i]])
      if value != "null":
        outstr += value.replace(".",",") + sep
      else:
        outstr += sep
  if len(outstr) > 0 and outstr[-1] == sep: outstr = outstr[:-1]
  return outstr

def generateMinMax(d):                                         # fill the min/max array with current values and send via UDP
  if not thisDay(min_max["minmax_init"]):
    #logPrint("<DEBUG> new day - reinitialize")
    if CSV_DAYFILE != "":
      more_daily = ["lightning_num","pm25_24h_co2","pm25_AQI_24h_co2","pm25_AQIlvl_24h_co2","pm10_24h_co2","pm10_AQI_24h_co2","pm10_AQIlvl_24h_co2","co2_24h"]
      for i in range(1,5):
        more_daily.append("pm25_avg_24h_ch"+str(i))
        more_daily.append("pm25_AQI_avg_24h_ch"+str(i))
        more_daily.append("pm25_AQIlvl_avg_24h_ch"+str(i))
      more_daily.append("dateutc")
      try:
        # check if CSV-dayfile exists and create the file with header
        if not os.path.exists(CSV_DAYFILE):
          mmhdr = "daytime;"+dictToString(min_max,";",False,[],[""],True,False,True) + ";"+moreFields(more_daily,last_d_m,";",True)
          if myDebug: sndPrint("<INFO> "+mmhdr)
          with open(CSV_DAYFILE, 'w') as csvdayfile: csvdayfile.write(mmhdr+"\n")
      except:
        logPrint("<ERROR> error while writing header to CSV-dayfile "+CSV_DAYFILE)
      try:
        # write values to the CSV-dayfile
        mmstr = minmaxCSVline() + ";"+moreFields(more_daily,last_d_m,";",False)
        if myDebug: sndPrint("<INFO> "+mmstr)
        with open(CSV_DAYFILE, "a+") as csvdayfile: csvdayfile.write(mmstr+"\n")
      except:
        logPrint("<ERROR> error while writing data to CSV-dayfile "+CSV_DAYFILE)
    # possibility to send the day-data via UDP or do anything else before resetting the min/max values
    # 
    initMinMax()
  # finally create UDPstr and send min/max data via UDP
  is_time = str(int(time.time()))
  UDPstr = ""
  UDPstr += calcMinMax(getfromDict(d,["baromrelhpa"]),"baromrelhpa",is_time)
  UDPstr += calcMinMax(getfromDict(d,["humidity"]),"humidity",is_time)
  UDPstr += calcMinMax(getfromDict(d,["tempc"]),"tempc",is_time)
  UDPstr += calcMinMax(getfromDict(d,["windchillc"]),"windchillc",is_time)
  UDPstr += calcMinMax(getfromDict(d,["heatindexc"]),"heatindexc",is_time)
  UDPstr += calcMinMax(getfromDict(d,["feelslikec"]),"feelslikec",is_time)
  UDPstr += calcMinMax(getfromDict(d,["dewptc"]),"dewptc",is_time)
  UDPstr += calcMinMax(getfromDict(d,["tempinc"]),"tempinc",is_time)
  UDPstr += calcMinMax(getfromDict(d,["humidityin","indoorhumidity"]),"humidityin",is_time)
  for i in range(1,9):
    UDPstr += calcMinMax(getfromDict(d,["temp"+str(i)+"c"]),"temp"+str(i)+"c",is_time)
    UDPstr += calcMinMax(getfromDict(d,["humidity"+str(i)]),"humidity"+str(i),is_time)
  UDPstr += calcMinMax(getfromDict(d,["tc_co2"]),"tc_co2",is_time)
  UDPstr += calcMinMax(getfromDict(d,["humi_co2"]),"humi_co2",is_time)
  for i in range(1,9):
    UDPstr += calcMinMax(getfromDict(d,["tf_ch"+str(i)+"c"]),"tf_ch"+str(i)+"c",is_time)
  UDPstr += calcMinMax(getfromDict(d,["windspeedkmh"]),"windspeedkmh",is_time)
  UDPstr += calcMinMax(getfromDict(d,["windgustkmh"]),"windgustkmh",is_time)
  UDPstr += calcMinMax(getfromDict(d,["solarradiation"]),"solarradiation",is_time)
  UDPstr += calcMinMax(getfromDict(d,["uv"]),"uv",is_time)
  UDPstr += calcMinMax(getfromDict(d,["sunmins"]),"sunmins",is_time)
  UDPstr += calcMinMax(getfromDict(d,["rainratemm"]),"rainratemm",is_time)
  UDPstr += calcMinMax(getfromDict(d,["dailyrainmm"]),"dailyrainmm",is_time)
  for i in range(1,9):
    UDPstr += calcMinMax(getfromDict(d,["soilmoisture"+str(i)]),"soilmoisture"+str(i),is_time)
  if len(UDPstr) > 0 and UDPstr[-1] == " ": UDPstr = UDPstr[:-1]
  if UDP_MINMAX and UDPstr != "": sendUDP("SID=" + defSID + " " + UDPstr)

def checkConfigFile(configname):
  errstr = ""
  a = []
  freeNr = nextNr = ln = 0
  for i in range(0, maxfwd+1): a.append("0")                   # +1 because stop not included
  if os.path.exists(CONFIG_FILE):
    with open(configname,"r") as infile:
      for line in infile:
        ln += 1
        if "[Forward-" in line:
          fpos = line.find("[Forward-")
          if "#" in line: errstr += "<ERROR> there must no \"#\" in lines with section names! (line "+str(ln)+")!\n"
          else:
            index = int(line[fpos+9:line.find("]")])
            if a[index] == "0":
              a[index] = "1"
            else: errstr += "<ERROR> there's already a section [Forward-"+str(index)+"] in the config file (line "+str(ln)+")!\n"
    for i in range(1,len(a)):                                    # first free number
      if a[i] == "0": # and i <= nextNr:
        freeNr = i
        break
    for i in range(len(a)-1,-1,-1):                               # next number
      if a[i] == "1":
        nextNr = i+1
        break
    if i >= maxfwd: nextNr = 0
    elif i == 0: nextNr = 1
    if nextNr == freeNr: freeNr = 0
  else: errstr += "<ERROR> file "+configname+" not found!"
  return(errstr, str(nextNr), str(freeNr))

# ------------------------------------------------------------
# main
# ------------------------------------------------------------
# Option abfragen; moeglich sind:
# -getWSIP, -getWSPORT, -createConfig, -autoConfig -patchW4L -recoverW4L
# -getWSconfig, -checkLBUPort, -checkLBHPort, -getCSVHEADER (mit Config-File)
try:
  option = sys.argv[1].upper()
except:
  option = ""
  pass

# das Config-File ist fuer scanWS, getWSIP, getWSPORT und getCSVheader nicht noetig, daher zuerst:
if option == '-SCANWS':
  scanWS()
  sys.exit(0)
elif option == '-GETWSIP':
  print(getWSconfig("IP"))
  sys.exit(0)
elif option == '-GETWSPORT':
  print(getWSconfig("PORT"))
  sys.exit(0)
elif option == '-SETWSINTERVAL':
  if len(sys.argv) == 5:
    loglog = False
    myDebug = True
    print(setWSconfig(sys.argv[2],sys.argv[3],'-','-',sys.argv[4]))
  else:
    print("you have to call -setWSInterval with additional parameters WS_IP WS_PORT WS_INTERVAL")
  sys.exit(0)
elif option == '-CREATECONFIG' or option == '-AUTOCONFIG':
  FOSHK_CONFIG = ""
  if option == '-CREATECONFIG':
    # Parameter targetip targetport myport
    if len(sys.argv) >= 8:
      # createConfig=`./foshkplugin.py -createConfig $WS_IP $WS_PORT $LB_IP $LBH_PORT $WS_INTERVAL`
      WS_IP = sys.argv[2]
      WS_PORT = sys.argv[3]
      LB_IP = sys.argv[4]
      if LB_IP == "none": LB_IP = ""
      LBH_PORT = sys.argv[5]
      WS_INTERVAL = sys.argv[6]
      LOX_IP = sys.argv[7]
      LOX_PORT = sys.argv[8]
      SVC_NAME = sys.argv[9] if len(sys.argv) >= 9 else ""
      # auto - but perhaps better to import as argv[10]?
      tries = 100
      v = 0
      LBU_PORT = 12340
      while not checkLBPort("",LBU_PORT,"UDP") and v <= tries:
        LBU_PORT+=1
        v += 1
      LBU_PORT = "" if v > tries else str(LBU_PORT)
      LOX_TIME = False
      UDP_ENABLE = True if LOX_IP != "none" and LOX_PORT != "none" else False
    else:
      print("you have to call -createConfig with additional parameters WS_IP WS_PORT LB_IP LBH_PORT WS_INTERVAL LOX_IP LOX_PORT SVC_NAME")
      sys.exit(0)
  else:                                          # -autoConfig
    WS_IP = getWSconfig("IP")
    WS_PORT = getWSconfig("PORT")
    WS_INTERVAL = getWSINTERVAL(WS_IP,WS_PORT)
    UDP_ENABLE = True
    # 10 ports may be a bit short, so try next 100 ports
    tries = 100
    v = 0
    LBH_PORT = 8080
    while not checkLBPort("",LBH_PORT,"TCP") and v <= tries:
      LBH_PORT+=1
      v += 1
    LBH_PORT = "" if v > tries else str(LBH_PORT)
    v = 0
    LBU_PORT = 12340
    while not checkLBPort("",LBU_PORT,"UDP") and v <= tries:
      LBU_PORT+=1
      v += 1
    LBU_PORT = "" if v > tries else str(LBU_PORT)
    # pruefen, ob LoxBerry installiert ist
    LB_IP = ""                                                 # frei lassen
    LOX_IP = ""
    LOX_PORT = LBU_PORT                                        # Loxone reacts on same port for VI and VO - danger?
    LOX_TIME = False
    try:
      CONFIG_FILE = os.environ.get("LBSCONFIG")+"/general.cfg"
      config = readConfigFile(CONFIG_FILE)
      LOX_IP = config.get('MINISERVER1','IPADDRESS',fallback='')
    except: pass
    # Ort der foshkplugin.conf festlegen, im LB-Betrieb dir aus LB-Config holen, ansonsten ""
    SVC_NAME = sys.argv[2] if len(sys.argv) >= 3 else "foshkplugin"
    FOSHK_CONFIG = checkLBP_PATH(SVC_NAME,"lbpconfigdir")
    if LOX_IP != "":                                           # LoxBerry ist installiert, MS bekannt
      LOX_TIME = True                                          # True, wenn $LBSCONFIG gesetzt, sonst False
  # all fields filled, now write config-file - but how to deal with updating a already running configuration?
  # autoConfigure must ONLY be started while first-time installation
  # gibt es im Config-File bereits ein ENABLED dann nicht!
  if FOSHK_CONFIG == "":                                       # if MS is not yet configured in LoxBerry or no LoxBerry-installation at all
    FOSHK_CONFIG = os.path.dirname(__file__) + "/"             # use running dir of foshkplugin.py
  CONFIG_FILE = FOSHK_CONFIG+"foshkplugin.conf"
  config = readConfigFile(CONFIG_FILE)
  if option == '-CREATECONFIG' or not config.has_option("Config","ENABLED"):
    if not config.has_section("Config") :
      config.add_section('Config')
    config.set('Config', 'LB_IP', LB_IP)
    config.set('Config', 'LBH_PORT', LBH_PORT)
    config.set('Config', 'LBU_PORT', LBU_PORT)
    config.set('Config', 'LOX_IP', LOX_IP)
    config.set('Config', 'LOX_PORT', LOX_PORT)
    config.set('Config', 'LOX_TIME', LOX_TIME)
    config.set('Config', 'UDP_ENABLE', UDP_ENABLE)
    config.set('Config', 'SVC_NAME', SVC_NAME)
    if not config.has_section("Weatherstation") :
      config.add_section('Weatherstation')
    config.set('Weatherstation', 'WS_IP', WS_IP)
    config.set('Weatherstation', 'WS_PORT', WS_PORT)
    config.set('Weatherstation', 'WS_INTERVAL', WS_INTERVAL)
    with open(CONFIG_FILE, 'w') as configfile: config.write(configfile)
    print("wrote your settings to " + CONFIG_FILE + ", ws: " + WS_IP + ":" + WS_PORT + " will send values every " + WS_INTERVAL + "secs to " + LB_IP + ":" + LBH_PORT + "; " + prgname + " sends UDP-datagrams to " + LOX_IP + ":" + LOX_PORT)
  else:
    print("autoConfig is allowed only in unconfigured state - remove ENABLED-line in config!")
  sys.exit(0)
elif option == "-CHECKCONFIG":
  SVC_NAME = sys.argv[2] if len(sys.argv) >= 3 else "foshkplugin"
  FOSHK_CONFIG = checkLBP_PATH(SVC_NAME,"lbpconfigdir")
  if FOSHK_CONFIG == "":                                       # not found in LB database
    if SVC_NAME == "foshkplugin":                              # no parameter set
      CONFIG_FILE = os.path.dirname(__file__) + "/foshkplugin.conf"
    else:                                                      # must be a filename
      CONFIG_FILE = SVC_NAME
  else:                                                        # found in LB database
    CONFIG_FILE = FOSHK_CONFIG+"foshkplugin.conf"
  err, nextNr, freeNr = checkConfigFile(CONFIG_FILE)
  print()
  print("checking config file "+CONFIG_FILE)
  for x in err.split("\n"): print(x)
  if freeNr != "0": print("free section: [Forward-"+freeNr+"]")
  else: print("no free section available!")
  if nextNr != "0": print("next section: [Forward-"+nextNr+"]")
  else: print("no next section available!")
  print()
  sys.exit(0)
elif option == "HELP" or option == "-HELP" or option == "--HELP" or option == "?" or option == "-?" or option == "-h":
  print()
  print("Phantasoft " + prgname + " " + prgver)
  print()
  print("creates a local web server to receive data from a local weather station and resend this different ways")
  print()
  print("possible parameters are:")
  print()
  print("-help                                     this help")
  print("-checkLBUPort portnumber                  print if port is available to bind as UDP port")
  print("-checkLBHPort portnumber                  print if port is available to bind as http port")
  print("-getCSVHEADER                             print the last known CSV file header")
  print("-scanWS                                   scan for all weather stations in local network")
  print("-getWSIP                                  search for weather station and output its ip address")
  print("-getWSPORT                                search for weather station and output its command port")
  print("-getWSINTERVAL [ipaddress port]           print weather station's interval of sending")
  print("-setWSINTERVAL [ipaddress port interval]  set weather station's interval of sending")
  print("-setWSconfig parameters                   write configuration from parameters to weather station")
  print("-writeWSconfig                            write configuration from config-file to weather station")
  print("-createConfig                             create default config file foshkplugin.conf in current dir")
  print("-autoConfig                               create config file foshkplugin.conf with auto discovery")
  print("-checkConfig [configfile]                 check current foshkplugin.conf or specified config file")
  print("-patchW4L                                 patch a W4L-installation to retrieve data from " + prgname)
  print("-recoverW4L                               restore the original W4L-configuration before patching")
  print()
  sys.exit(0)

# Config-File finden
# search the Config-File for FOSHKplugin - defaults to start-path
CONFIG_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_FILE = CONFIG_DIR+"/foshkplugin.conf"
if not os.path.isfile(CONFIG_FILE) and "LBPCONFIG" in os.environ:
  svcname = "REPLACEFOSHKPLUGINSERVICE"                        # must be R.E.P.L.A.C.E.F.O.S.H.K.P.L.U.G.I.N.S.E.R.V.I.C.E without dots for sed/LB-version
  if svcname == "REPLACE"+"FOSHK"+"PLUGIN"+"SERVICE": svcname = "foshkplugin"
  CONFIG_DIR = os.environ.get("LBPCONFIG")+"/"+svcname
  CONFIG_FILE = CONFIG_DIR+"/foshkplugin.conf"

if not os.path.isfile(CONFIG_FILE):
  # no configuration file found!
  logPrint("<ERROR> configuration file " + CONFIG_FILE + " not found!")
  sys.exit(0)

# Konfiguration einlesen
config = readConfigFile(CONFIG_FILE)

SVC_NAME = config.get('Config','SVC_NAME',fallback='foshkplugin')
LOX_IP = config.get('Config','LOX_IP',fallback='LOX_IP')
LOX_PORT = config.get('Config','LOX_PORT',fallback='LOX_PORT')
LB_IP = config.get('Config','LB_IP',fallback='LB_IP')
LBU_PORT = config.get('Config','LBU_PORT',fallback='LBU_PORT')
LBH_PORT = config.get('Config','LBH_PORT',fallback='LBH_PORT_DEFAULT')
LOX_TIME = mkBoolean(config.get('Config','LOX_TIME',fallback="False"))
USE_METRIC = mkBoolean(config.get('Config','USE_METRIC',fallback="True"))
IGNORE_EMPTY = mkBoolean(config.get('Config','IGNORE_EMPTY',fallback="True"))
UDP_ENABLE = mkBoolean(config.get('Config','UDP_ENABLE',fallback="True"))
UDP_IGNORE = config.get('Config',"UDP_IGNORE",fallback="").replace("\"","").replace(" ","").split(",")
# v0.07: override default SID
SID = config.get('Config','DEF_SID',fallback=defSID).replace("\"","")
defSID = SID if SID != "" else defSID
UDP_STATRESEND = config.get('Config','UDP_STATRESEND',fallback="0").replace("\"","")
# v0.08 enable remote restart/reboot
REBOOT_ENABLE = mkBoolean(config.get('Config','REBOOT_ENABLE',fallback="False"))
RESTART_ENABLE = mkBoolean(config.get('Config','RESTART_ENABLE',fallback="False"))
HIDDEN_FEATURES = mkBoolean(config.get('Config','HIDDEN_FEATURES',fallback="False"))
WS_IP = config.get('Weatherstation','WS_IP',fallback='WS_IP')
WS_PORT = config.get('Weatherstation','WS_PORT',fallback='WS_PORT')
WS_INTERVAL = config.get('Weatherstation','WS_INTERVAL',fallback='60')
WS90_CONVERT = mkBoolean(config.get('Weatherstation','WS90_CONVERT',fallback='True'))
CSV_NAME = config.get('CSV','CSV_NAME',fallback='CSV_NAME')
CSV_INTERVAL = config.get('CSV','CSV_INTERVAL',fallback='CSV_INTERVAL')
CSV_FIELDS = config.get('CSV','CSV_FIELDS',fallback='CSV_FIELDS').replace("\"","")
CSV_DAYFILE = config.get('CSV','CSV_DAYFILE',fallback='')
EVAL_VALUES = mkBoolean(config.get('Export','EVAL_VALUES',fallback="False"))
ADD_ITEMS = config.get('Export','ADD_ITEMS',fallback='').replace("\"","")
WSDOG_WARNING = mkBoolean(config.get('Warning','WSDOG_WARNING',fallback="True"))
WSDOG_INTERVAL = config.get('Warning','WSDOG_INTERVAL',fallback='3')
WSDOG_RESTART = config.get('Warning','WSDOG_RESTART',fallback='0')
STORM_WARNING = mkBoolean(config.get('Warning','STORM_WARNING',fallback="True"))
STORM_WARNDIFF = config.get('Warning','STORM_WARNDIFF',fallback='1.75')
STORM_WARNDIFF3H = config.get('Warning','STORM_WARNDIFF3H',fallback='3.75')
STORM_EXPIRE = config.get('Warning','STORM_EXPIRE',fallback='60')
SENSOR_WARNING = mkBoolean(config.get('Warning','SENSOR_WARNING',fallback="False"))
SENSOR_MANDATORY = config.get('Warning','SENSOR_MANDATORY',fallback='').replace("\"","")
# ab v0.06 bei vorhandenem WH57/DP60 (Blitzwarner) aktiv:
TSTORM_WARNING = mkBoolean(config.get('Warning','TSTORM_WARNING',fallback="True"))
TSTORM_WARNCOUNT = config.get('Warning','TSTORM_WARNCOUNT',fallback='1')
TSTORM_WARNDIST = config.get('Warning','TSTORM_WARNDIST',fallback='20')
TSTORM_EXPIRE = config.get('Warning','TSTORM_EXPIRE',fallback='30')
# ab v0.06 battery warning
BATTERY_WARNING = mkBoolean(config.get('Warning','BATTERY_WARNING',fallback="True"))
# ab v0.06 save some states for resurrection
inWStimeoutWarning = mkBoolean(config.get('Status','inWStimeoutWarning',fallback="False"))
inSensorWarning = mkBoolean(config.get('Status','inSensorWarning',fallback="False"))
SensorIsMissed = config.get('Status','SensorIsMissed',fallback="")
inBatteryWarning = mkBoolean(config.get('Status','inBatteryWarning',fallback="False"))
inStormWarning = mkBoolean(config.get('Status','inStormWarning',fallback="False"))
inStorm3h = mkBoolean(config.get('Status','inStorm3h',fallback="False"))
inStormWarnStart = config.get('Status','inStormWarnStart',fallback="")
inStormTime = config.get('Status','inStormTime',fallback="")
inTSWarning = mkBoolean(config.get('Status','inTSWarning',fallback="False"))
inTSWarnStart = config.get('Status','inTSWarnStart',fallback="")
#last_lightning_time = config.get('Status','last_lightning_time',fallback="")
inTS_lightning_num = config.get('Status','inTS_lightning_num',fallback="0")
lastStopTime = config.get('Status','StopTime',fallback="")
# ab v0.06 Sprache im Config-File festlegbar (fuer generic-Version)
LANGUAGE = config.get('Config','LANGUAGE',fallback='').replace("\"","")
# ab v0.06 simple authentication-mechanism
AUTH_PWD = config.get('Config','AUTH_PWD',fallback='').replace("\"","")
# ab v0.06 fake outdoor sensor with internal values
fakeOUT_TEMP = config.get('Export','OUT_TEMP',fallback='').replace("\"","")
fakeOUT_HUM = config.get('Export','OUT_HUM',fallback='').replace("\"","")
# ab v0.07 exchange incoming time string with local receiving time
exchangeTime = mkBoolean(config.get('Export','OUT_TIME',fallback="False"))
# v0.09 adjust missing http:// in fwd_url automatically
URL_REPAIR = mkBoolean(config.get('Export','URL_REPAIR',fallback="True"))
# v0.07: use Pushover for push warnings
PO_ENABLE = mkBoolean(config.get('Pushover','PO_ENABLE',fallback="False"))
PO_URL = config.get('Pushover','PO_URL',fallback='https://api.pushover.net/1/messages.json').replace("\"","")
if PO_URL == "": PO_URL = "https://api.pushover.net/1/messages.json"
PO_TOKEN = config.get('Pushover','PO_TOKEN',fallback='').replace("\"","")
PO_USER = config.get('Pushover','PO_USER',fallback='').replace("\"","")
# v0.07: prevent lines containing substrings to write to logfile - v0.08: another name for same function
LOG_IGNORE = config.get('Logging',"LOG_IGNORE",fallback=config.get('Logging',"IGNORE_LOG",fallback="")).replace("\"","").split(",")
BUT_PRINT = mkBoolean(config.get('Logging','BUT_PRINT',fallback="True"))
LOG_ENABLE = mkBoolean(config.get('Logging','LOG_ENABLE',fallback="True"))
LEAKAGE_WARNING = mkBoolean(config.get('Warning','LEAKAGE_WARNING',fallback="False"))
inLeakageWarning = mkBoolean(config.get('Status','inLeakageWarning',fallback="False"))
# v0.08 CO2 warning
CO2_WARNING = mkBoolean(config.get('Warning','CO2_WARNING',fallback="False"))
inCO2Warning = mkBoolean(config.get('Status','inCO2Warning',fallback="False"))
CO2_WARNLEVEL = config.get('Warning','CO2_WARNLEVEL',fallback='1200')
# v0.07 - fix keys lightning_time & lightning without a value (for GW1000) - default True
FIX_LIGHTNING = mkBoolean(config.get('Export','FIX_LIGHTNING',fallback="True"))  
last_lightning_time = config.get('Status','last_lightning_time',fallback="")
last_lightning = config.get('Status','last_lightning',fallback="")
# v0.08 # send min/max values via UDP if UDP sending is enabled
UDP_MINMAX = mkBoolean(config.get('Export','UDP_MINMAX',fallback="True"))
# v0.08: max length of outgoing UDP packet; will be fragmented if longer than this value
UDP_MAXLEN = config.get('Config','UDP_MAXLEN',fallback=config.get('Config','UDP_LEN',fallback=''))
try:
  UDP_MAXLEN = int(UDP_MAXLEN)
except ValueError:
  UDP_MAXLEN = 2000
  pass
if UDP_MAXLEN < 128: UDP_MAXLEN = 128
# v0.08 coordinates
COORD_LAT = config.get('Coordinates','LAT',fallback="").replace(",",".")
COORD_LON = config.get('Coordinates','LON',fallback="").replace(",",".")
COORD_ALT = config.get('Coordinates','ALT',fallback="").replace(",",".")
# v0.08: log level
LOG_LEVEL = config.get('Logging','LOG_LEVEL',fallback='ALL').upper()
if LOG_LEVEL not in [ "ERROR", "WARNING", "INFO", "ALL" ]: LOG_LEVEL = "ALL"
# v0.09: Sunduration
useSunCalc = mkBoolean(config.get('Sunduration','SUN_CALC',fallback="False"))
SUN_MIN = config.get('Sunduration','SUN_MIN',fallback=0)       ##
SUN_COEF = config.get('Sunduration','SUN_COEF',fallback=0.8)   ##

# v0.09: Interval warning
INTVL_WARNING = mkBoolean(config.get('Warning','INTVL_WARNING',fallback="False"))
INTVL_PCT = config.get('Warning','INTVL_PCT',fallback='10')    # percent

# for firmware update check
UPD_CHECK = mkBoolean(config.get('Update','UPD_CHECK',fallback="True"))
UPD_INTERVAL = config.get('Update','UPD_INTERVAL',fallback='86400').replace("\"","")
UPD_URL = config.get('Update','UPD_URL',fallback='http://download.ecowitt.net/down/filewave?v=FirwaveReadme.txt').replace("\"","")
if UPD_CHECK:
  try:
    UPD_INTERVAL = int(UPD_INTERVAL)
  except ValueError:
    UPD_INTERVAL = 0
    UPD_CHECK = False
    pass

if SENSOR_WARNING and SENSOR_MANDATORY != "":
  SENSOR_MANDATORY = SENSOR_MANDATORY.replace(" ","").strip("\"")
  senmand_arr = SENSOR_MANDATORY.split(",")
else:
  SENSOR_WARNING = False

# etwaige Anfuehrungszeichen entfernen
#CSV_FIELDS = CSV_FIELDS.replace("\"","")
#print("CSV-Fields: " + CSV_FIELDS)

try: UDP_STATRESEND = int(UDP_STATRESEND)
except ValueError: UDP_STATRESEND = 0                          # default: no regular resend of status

try: WSDOG_INTERVAL = int(WSDOG_INTERVAL)
except ValueError: WSDOG_INTERVAL = 3                          # default: warn after 3 intervals

try: WSDOG_RESTART = int(WSDOG_RESTART)
except ValueError: WSDOG_RESTART = 0                           # default: do not restart the plugin

try: STORM_WARNDIFF = float(STORM_WARNDIFF)
except ValueError: STORM_WARNDIFF = float(1.75)                # default: 1.75hPa

try: STORM_WARNDIFF3H = float(STORM_WARNDIFF3H)
except ValueError: STORM_WARNDIFF3H = float(3.75)              # default: 3.75hPa

try: STORM_EXPIRE = int(STORM_EXPIRE)
except ValueError: STORM_EXPIRE = 60                           # default: 60 minutes

try: TSTORM_WARNCOUNT = int(TSTORM_WARNCOUNT)
except ValueError: TSTORM_WARNCOUNT = 1                        # default: 1 lightning

try: TSTORM_WARNDIST = int(TSTORM_WARNDIST)
except ValueError: TSTORM_WARNDIST = 30                        # default: 30km

try: TSTORM_EXPIRE = int(TSTORM_EXPIRE)
except ValueError: TSTORM_EXPIRE = 15                          # default: 15 minutes

try: lastStopTime = int(lastStopTime)
except ValueError: lastStopTime = 0

try: inStormWarnStart = int(inStormWarnStart)
except ValueError: inStormWarnStart = 0

try: inStormTime = int(inStormTime)
except ValueError: inStormTime = 0

try: inTSWarnStart = int(inTSWarnStart)
except ValueError: inTSWarnStart = 0

try: last_lightning_time = int(last_lightning_time)
except ValueError: last_lightning_time = 0

try: inTS_lightning_num = int(inTS_lightning_num)
except ValueError: inTS_lightning_num = 0

# in case autoconfig failed or WS_INTERVAL was set wrong reset it to a numerical value
try: int(WS_INTERVAL)
except ValueError: WS_INTERVAL="60"

# real sending interval checking
try: int(INTVL_PCT)
except ValueError: INTVL_PCT="10"
try: INTVL_LIMIT = math.ceil((int(WS_INTERVAL)+int(INTVL_PCT)/100*int(WS_INTERVAL)))
except ValueError: INTVL_LIMIT = int(WS_INTERVAL)*1.1          # fallback: 10%

fwd_error = ""
fwd_arr = []
forwardMode = False
for i in range(0, maxfwd+1):                                   # +1 because stop not included
  section = "Forward" if i == 0 else "Forward-"+str(i)
  if config.has_section(section):
    fwd_enable = mkBoolean(config.get(section,"FWD_ENABLE",fallback="True"))
    fwd_cmt = config.get(section,"FWD_CMT",fallback="")        # v0.07: possibility to comment this forward
    fwd_url = config.get(section,"FWD_URL",fallback="")
    fwd_interval = config.get(section,"FWD_INTERVAL",fallback=WS_INTERVAL)
    fwd_ignore = config.get(section,"FWD_IGNORE",fallback="").replace("\"","").replace(" ","").split(",")
    fwd_type = config.get(section,"FWD_TYPE",fallback="WU").replace("\"","")
    fwd_sid = config.get(section,"FWD_SID",fallback="").replace("\"","")
    fwd_pwd = config.get(section,"FWD_PWD",fallback="").replace("\"","")
    fwd_status = mkBoolean(config.get(section,"FWD_STATUS",fallback="False"))
    fwd_minmax = mkBoolean(config.get(section,"FWD_MINMAX",fallback="False"))
    fwd_exec = config.get(section,"FWD_EXEC",fallback="").replace("\"","")
    fwd_mqttcycle = config.get(section,"FWD_MQTTCYCLE",fallback="0").replace("\"","")
    fwd_nr = str(i) if i > 9 else "0"+str(i)                   # for logging - qualifies the corresponding forward
    fwd_last = 0                                               # last forward-time
    fwd_remap = config.get(section,"FWD_REMAP",fallback="").replace("\"","")
    try:
      d_remap = dict(x.split("=") for x in fwd_remap.split(",")) if fwd_remap != "" else {}
    except ValueError:
      d_remap = {}
      fwd_error = "<WARNING> you have to separate pairs in FWD_REMAP with \",\" - remapping disabled for "+section
      pass
    try: fwd_interval_num = int(fwd_interval)
    except ValueError: fwd_interval_num = 0
    try: fwd_mqttcycle = int(fwd_mqttcycle)
    except ValueError: fwd_mqttcycle = 0
    if fwd_enable and (fwd_url != "" or fwd_exec != ""):       # v0.07: enable/disable manually
      # v0.09 repair broken target URL for WU, RAW, EW, RAWEW, EWRAW, LD, RAWCSV, CSVRAW, CSV, AMB, RAWAMB, AMBRAW, MT, WC, AWEKAS, WETTERCOM, WEATHER365, WETTERSEKTOR
      if URL_REPAIR and fwd_type in ["WU", "RAW", "EW", "RAWEW", "EWRAW", "LD", "RAWCSV", "CSVRAW", "CSV", "AMB", "RAWAMB", "AMBRAW", "MT", "WC", "AWEKAS", "WETTERCOM", "WEATHER365", "WETTERSEKTOR"]:
        if fwd_url != "" and fwd_exec == "" and not (fwd_url[:7] == "http://" or fwd_url[:8] == "https://"):
          fwd_url = "http://"+fwd_url
          if fwd_error != "": fwd_error+="\n"
          fwd_error += "<WARNING> FWD-"+fwd_nr+": URL must start with \"http://\" - adapted to "+fwd_url
      fwd_arr.append([fwd_url,fwd_interval,fwd_interval_num,fwd_last,fwd_ignore,fwd_type,fwd_sid,fwd_pwd,fwd_status,fwd_minmax,fwd_exec,fwd_nr,fwd_mqttcycle,d_remap])
      forwardMode = True

CSVsave = True if CSV_NAME != '' and CSV_FIELDS != '' else False
try: CSV_INTERVAL_num = int(CSV_INTERVAL)
except ValueError: CSV_INTERVAL_num = 0

logfile = config.get('Logging','logfile',fallback='')
rawfile = config.get('Logging','rawfile',fallback='')
sndfile = config.get('Logging','sndfile',fallback='')

loglog = True if logfile != '' and 'REPLACEFOSHKPLUGINLOGDIR' not in logfile and LOG_ENABLE else False
rawlog = True if rawfile != '' and 'REPLACEFOSHKPLUGINLOGDIR' not in rawfile and LOG_ENABLE else False
sndlog = True if sndfile != '' and 'REPLACEFOSHKPLUGINLOGDIR' not in sndfile and LOG_ENABLE else False

# first file logger
if loglog :
  try:
    logger = setup_logger('std_logger',logfile,format=formatter)
  except:
    print("### can not log std_logger to "+logfile)
    loglog = False
    pass

# raw-Logger
myformatter = logging.Formatter('%(asctime)s.%(msecs)03d %(message)s',datefmt="%d.%m.%Y %H:%M:%S")
if rawlog :
  try:
    rawlogger = setup_logger('raw_logger',rawfile,format=myformatter)
  except:
    logPrint("<ERROR> can not log raw_logger to "+rawfile)
    rawlog = False
    pass

# send-Logger
if sndlog :
  try:
    sndlogger = setup_logger('snd_logger',sndfile,format=myformatter)
  except:
    logPrint("<ERROR> can not log snd_logger to "+sndfile)
    sndlog = False
    pass

# fuer diese Funktionen sind IP-Adresse und Port noetig, daher erst nach Einlesen der Config moeglich
if option == '-SETWSCONFIG':
  if len(sys.argv) == 7:
    #ws_ipaddr, ws_port, custom_host, custom_port, custom_interval
    logPrint(setWSconfig(sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],sys.argv[6]))
  else:
    print("you have to call -setWSconfig with additional parameters WS_IP WS_PORT LB_IP LBH_PORT WS_INTERVAL")
  sys.exit(0)
elif option == '-CHECKLBUPORT':
  if len(sys.argv) >= 3 and sys.argv[2].isnumeric():
    myLB_IP = LB_IP if LB_IP != "" else socket.gethostbyname(socket.gethostname())
    if LBU_PORT == "": LBU_PORT = sys.argv[2]
    if checkLBPort("",int(sys.argv[2]),"UDP") or (int(sys.argv[2]) == int(LBU_PORT) and FOSHKpluginGetStatus("http://"+myLB_IP+":"+LBH_PORT+"/FOSHKplugin/LBU_PORT") == LBU_PORT):
      print("ok")
    else:
      print("failed")
  else:
    print("you have to call -checkLBUPort with additional parameter: PORT")
  sys.exit(0)
elif option == '-CHECKLBHPORT':
  if len(sys.argv) >= 3 and sys.argv[2].isnumeric():
    myLB_IP = LB_IP if LB_IP != "" else socket.gethostbyname(socket.gethostname())
    if LBH_PORT == "": LBH_PORT = sys.argv[2]
    if checkLBPort("",int(sys.argv[2]),"TCP") or (int(sys.argv[2]) == int(LBH_PORT) and FOSHKpluginGetStatus("http://"+myLB_IP+":"+LBH_PORT+"/FOSHKplugin/state") == "running"):
      print("ok")
    else:
      print("failed")
  else:
    print("you have to call -checkLBHPort with additional parameter: PORT")
  sys.exit(0)
elif option == '-GETWSINTERVAL':
  if len(sys.argv) == 4:
    WS_IP = sys.argv[2]
    WS_PORT = sys.argv[3]
  elif WS_IP == "" or WS_PORT == "":
    WS_IP = getWSconfig("IP")
    WS_PORT = getWSconfig("PORT")
  print(getWSINTERVAL(WS_IP,WS_PORT))
  sys.exit(0)
elif option == '-PATCHW4L':
  w4lconfigdir = checkLBP_PATH("weather4lox","lbpconfigdir")
  CONFIG_FILE = w4lconfigdir+"weather4lox.cfg"
  if w4lconfigdir != "" or not os.path.exists(CONFIG_FILE):    # dir and file should exist - if not W4L is missing
    print("W4L is not installed - not patched!")
  else:
    config = readConfigFile(CONFIG_FILE)
    config.set('SERVER', 'LOCALGRABBER', "1")
    config.set('SERVER', 'WULOCALGRABBER', "0")
    myLB_IP = LB_IP if LB_IP != "" else socket.gethostbyname(socket.gethostname())
    if not config.has_section("LOCAL") : config.add_section('LOCAL')
    # authentication
    myAUTH = "?auth="+AUTH_PWD if AUTH_PWD != "" else ""
    config.set('LOCAL', 'URL', "http://"+myLB_IP+":"+LBH_PORT+"/w4l/current.dat"+myAUTH)
    if not config.has_section("WULOCAL") : config.add_section('WULOCAL')
    config.set('WULOCAL', 'URL', "http://"+myLB_IP+":"+LBH_PORT+"/observations/current/json/units=m"+myAUTH)
    print("set W4L SERVER\LOCALGRABBER=1 to use   " + "http://"+myLB_IP+":"+LBH_PORT+"/w4l/current.dat"+myAUTH)
    print("set W4L SERVER\WULOCALGRABBER=0 to use " + "http://"+myLB_IP+":"+LBH_PORT+"/observations/current/json/units=m"+myAUTH)
    with open(CONFIG_FILE, 'w') as configfile: config.write(configfile)
    w4lbindir = checkLBP_PATH("weather4lox","lbpbindir")
    #foshkbindir = checkLBP_PATH("foshkplugin","lbpbindir")
    foshkbindir = checkLBP_PATH(SVC_NAME,"lbpbindir")
    # create backup-file of fetch.pl only if not already existent
    backupfile = "fetch.pl.foshkbackup"
    if not os.path.exists(w4lbindir + backupfile):
      os.system("cp -fp " + w4lbindir + "fetch.pl " + w4lbindir + backupfile)
      print("backup-file " + backupfile + " has been created")
    # v0.05 - echtes Patchen der vorhandenen fetch.pl
    patched = False
    f_in = open(w4lbindir+"fetch.pl")
    f_out = open(foshkbindir+"fetch.pl","w")
    for line in f_in:
      if line.rstrip() == "# Grab some data from local Wunderground-server":
        patched = True
      elif not patched and line.rstrip() == "# Data to Loxone":
        f_out.write("# Grab some data from local Wunderground-server\n")
        f_out.write("if ( $pcfg->param(\"SERVER.WULOCALGRABBER\") ) {\n")
        f_out.write("        LOGINF \"Starting Grabber grabber_wu-local.pl\";\n")
        f_out.write("        $log->close;\n")
        f_out.write("        system (\"$lbpbindir/grabber_wu-local.pl $verbose_opt\");\n")
        f_out.write("        $log->open;\n")
        f_out.write("}\n")
        f_out.write("\n")
        f_out.write("# Grab current data from local weather station\n")
        f_out.write("if ( $pcfg->param(\"SERVER.LOCALGRABBER\") ) {\n")
        f_out.write("        LOGINF \"Starting Grabber grabber_local.pl\";\n")
        f_out.write("        $log->close;\n")
        f_out.write("        system (\"$lbpbindir/grabber_local.pl $verbose_opt\");\n")
        f_out.write("        $log->open;\n")
        f_out.write("}\n")
        f_out.write("\n")
      f_out.write(line)
    f_in.close()
    f_out.close()
    # neu erzeugte fetch.pl nach w4l kopieren
    a = os.system("cp -fp " + foshkbindir + "fetch.pl " + w4lbindir + "fetch.pl")
    # Symlinks -fps funktionieren leider nicht; grabber bringt dann Fehler Can't call method "param" on an undefined value at ./grabber_local.pl line 61.
    b = os.system("cp -fp " + foshkbindir + "grabber_local.pl " + w4lbindir)
    c = os.system("cp -fp " + foshkbindir + "grabber_wu-local.pl " + w4lbindir)
    if a+b+c == 0:
      print("W4L was patched successfully")
    else:
      print("there were problems while patching W4L ("+str(a)+"/"+str(b)+"/"+str(c)+")")
  sys.exit(0)
elif option == '-RECOVERW4L':
  w4lconfigdir = checkLBP_PATH("weather4lox","lbpconfigdir")
  CONFIG_FILE = w4lconfigdir+"weather4lox.cfg"
  if w4lconfigdir != "" or not os.path.exists(CONFIG_FILE):    # dir and file should exist - if not W4L is missing
    print("W4L is not installed - not recovered!")
  else:
    try:
      config = readConfigFile(CONFIG_FILE)
      config.remove_option('SERVER', 'LOCALGRABBER')
      config.remove_option('SERVER', 'WULOCALGRABBER')
      config.remove_option('LOCAL', 'URL')
      config.remove_option('WULOCAL', 'URL')
      config.remove_section('LOCAL')
      config.remove_section('WULOCAL')
      with open(CONFIG_FILE, 'w') as configfile: config.write(configfile)
    except:
      print("problems while restoring original config-file " + CONFIG_FILE)
      pass
    w4lbindir = checkLBP_PATH("weather4lox","lbpbindir")
    try:
      if os.path.exists(w4lbindir + "fetch.pl.foshkbackup"): os.system("mv -f " + w4lbindir + "fetch.pl.foshkbackup " + w4lbindir + "fetch.pl")
      if os.path.exists(w4lbindir + "grabber_local.pl"): os.system("rm -f " + w4lbindir + "grabber_local.pl")
      if os.path.exists(w4lbindir + "grabber_wu-local.pl"): os.system("rm -f " + w4lbindir + "grabber_wu-local.pl")
      print("original state of W4L was recovered")
    except:
      print("unable to recover original state in " + w4lbindir)
      pass
  sys.exit(0)
elif option == '-WRITEWSCONFIG':                               # write configuration from Config-file to WS
  myLB_IP = LB_IP if LB_IP != "" else socket.gethostbyname(socket.gethostname())
  #logPrint("setWSconfig("+WS_IP+","+WS_PORT+","+myLB_IP+","+LBH_PORT+","+WS_INTERVAL+")") if WS_IP != "" and WS_PORT != "" and myLB_IP != "" and LBH_PORT != "" and WS_INTERVAL != "" else print("error in configfile " + CONFIG_FILE + " - WS_IP, WS_PORT, LB_IP, LBH_PORT and WS_INTERVAL have to be specified!")
  logPrint("<OK> writeWSconfig: write settings from config-file to weather station")
  logPrint(setWSconfig(WS_IP, WS_PORT, myLB_IP, LBH_PORT, WS_INTERVAL)) if WS_IP != "" and WS_PORT != "" and myLB_IP != "" and LBH_PORT != "" and WS_INTERVAL != "" else print("error in configfile " + CONFIG_FILE + " - WS_IP, WS_PORT, LB_IP, LBH_PORT and WS_INTERVAL have to be specified!")
  sys.exit(0)
elif option == '-GETCSVHEADER':                                # v0.07 now only after reading config to read the right file
  hname = "/tmp/"+prgname+"-"+LBH_PORT+".csvheader"
  try:
    print(open(hname).read())
  except:
    print()
  sys.exit(0)

allPrint("<OK> "+prgname+" "+prgver+" started")

# v0.09 configuration file in use
logPrint("<OK> using configuration file " + CONFIG_FILE)

# v0.08 log level
logPrint("<OK> log level set to " + LOG_LEVEL + " (out of ERROR, WARNING, INFO, ALL (default))")

# get Language of LoxBerry:
#myLanguage = getLBLang()
myLanguage = getLBLang() if LANGUAGE == "" else LANGUAGE.upper()
# v0.06 set encoding for UDP and http-Out
OutEncoding = "ISO-8859-2" if myLanguage == "SK" else "ISO-8859-1"

# init stundenwerte for StormWarning
if STORM_WARNING:
  # read pickle-file for stundenwerte if possible
  # v0.07: for compatibility - rename old named pkl-file to newer name
  try:
    os.rename(CONFIG_DIR+"/"+prgname+"-stundenwerte.pkl",fname)
  except:
    pass
  fname = CONFIG_DIR+"/"+prgname+"-"+LBH_PORT+"-stundenwerte.pkl"
  if os.path.exists(fname) and int(time.time()) - lastStopTime < 600:                    # file exists; last stop < 10 minutes --> stundenwerte are current
    with open(fname, 'rb') as input:
      try:
        stundenwerte = pickle.load(input)
        if stundenwerte.maxlen != int(3*3600/int(WS_INTERVAL)):
          logPrint("<WARNING> deque-size mismatch (is: " + str(stundenwerte.maxlen) + " needed: " + str(int(3*3600/int(WS_INTERVAL))) + ") - recreate")
          raise ValueError("deque-size mismatch")
        logPrint("<OK> loaded stundenwerte from " + fname + " (" + str(len(stundenwerte)) + ")")
      except:
        stundenwerte = deque(maxlen=(int(3*3600/int(WS_INTERVAL))))
        logPrint("<WARNING> unable to load stundenwerte from " + fname)
        pass
  else:
    stundenwerte = deque(maxlen=(int(3*3600/int(WS_INTERVAL))))
  logPrint("<OK> storm warning activated, will warn if air pressure rises/drops more than " + str(STORM_WARNDIFF) + " hPa/hour or " + str(STORM_WARNDIFF3H) + "hPa/3hr with expiry time of " + str(STORM_EXPIRE) + " minutes")

# create wind-avg-deque
if EVAL_VALUES: wind_avg10m  = deque(maxlen=(int(10*60/int(WS_INTERVAL))))               # holds 10 minutes of speed, direction and windgust

# read minmax array from file if available
initMinMax()
initLen = len(min_max)
min_max_pickle = CONFIG_DIR+"/"+prgname+"-"+LBH_PORT+"-minmax.pkl"
loadMinMax(min_max_pickle)
if len(min_max) != initLen or not thisDay(min_max["minmax_init"]):
  logPrint("<WARNING> loaded min/max values are wrong - reinitialize")
  initMinMax()
  # rename current dayfile so a new CSV file will be created
  if os.path.exists(CSV_DAYFILE):
    try:
      extpos = CSV_DAYFILE.rfind(".")
    except ValueError: pass
    if extpos < 0: extpos = len(CSV_DAYFILE)
    new_CSV_DAYFILE = CSV_DAYFILE[:extpos]+"-"+time.strftime("%y%m%d%H%M%S",time.localtime())+CSV_DAYFILE[extpos:]
    try:
      os.rename(CSV_DAYFILE,new_CSV_DAYFILE)
    except: pass
    logPrint("<WARNING> current CSV dayfile " + CSV_DAYFILE + " renamed to " + new_CSV_DAYFILE)

# start InfiniteTimer for weather station watchdog
if WSDOG_WARNING:
  checkWS = InfiniteTimer(int(WS_INTERVAL), checkWS_report)
  checkWS.start()
  logPrint("<OK> report watchdog activated, will warn if weather station did not report within " + str(WSDOG_INTERVAL) + " send-intervals")
  if WSDOG_RESTART > WSDOG_INTERVAL: logPrint("<OK> " + prgname + " will restart if weather station did not report within " + str(WSDOG_RESTART) + " send-intervals")

if SENSOR_WARNING:
  logPrint("<OK> sensor warning activated, will warn if data for mandatory sensor " + str(senmand_arr) + " is missed")

if BATTERY_WARNING:
  logPrint("<OK> battery warning enabled, will warn if battery level of all known sensors is critical - to disable set BATTERY_WARNING = False in config")
else:
  logPrint("<OK> battery warning disabled - to enable set BATTERY_WARNING = True in config")

if TSTORM_WARNING:
  logPrint("<OK> thunderstorm warning activated, will warn if lightning sensor WH57 present, count of lightnings is more than " + str(TSTORM_WARNCOUNT) +" and distance is less or equal " + str(TSTORM_WARNDIST) + "km with expiry time of " + str(TSTORM_EXPIRE) + " minutes")
  #logPrint("<OK> thunderstorm warning activated, warning occurs when " + str(TSTORM_WARNCOUNT) + " lightning(s) are detected within a radius of " + str(TSTORM_WARNDIST) + "km (WH57 provided); expiry time: " + str(TSTORM_EXPIRE) + " minutes")

if LEAKAGE_WARNING:
  logPrint("<OK> leakage warning enabled, will warn if leakage detected on any WH55 - to disable set LEAKAGE_WARNING = False in config")
else:
  logPrint("<OK> leakage warning disabled - to enable set LEAKAGE_WARNING = True in config")

if CO2_WARNING:
  logPrint("<OK> CO2 warning enabled, will warn if CO2 value is higher than configured as CO2_WARNLEVEL (currently: "+CO2_WARNLEVEL+"ppm) - to disable set CO2_WARNING = False in config")
else:
  logPrint("<OK> CO2 warning disabled - to enable set CO2_WARNING = True in config")

if INTVL_WARNING:
  logPrint("<OK> interval warning enabled, will warn if the real sending interval is more than "+str(INTVL_LIMIT)+" ("+INTVL_PCT+"% above the defined value "+WS_INTERVAL+") - to disable set INTVL_WARNING = False in config")
else:
  logPrint("<OK> interval warning disabled - to enable set INTVL_WARNING = True in config")

if UDP_STATRESEND > 0:
  logPrint("<OK> resend warnings per UDP every " + str(UDP_STATRESEND) + " seconds")

if FIX_LIGHTNING:
  logPrint("<OK> automatic save/restore for lightning-data enabled - to disable set FIX_LIGHTNING = False in config")
else:
  logPrint("<OK> automatic save/restore for lightning-data disabled - to enable set FIX_LIGHTNING = True in config")

if AUTH_PWD != "":
  logPrint("<OK> authentication-mode enabled, use the passphrase configured as AUTH_PWD")

if myDebug:
  logPrint("<OK> debug-mode activated - to disable set myDebug = False in foshkplugin.py")

if fakeOUT_TEMP != "":
  logPrint("<OK> using " + fakeOUT_TEMP + " as outdoor temperature \"tempf\" in Ecowitt-mode")

if fakeOUT_HUM != "":
  logPrint("<OK> using " + fakeOUT_HUM + " as outdoor humidity \"humidity\" in Ecowitt-mode")

if exchangeTime:
  logPrint("<OK> exchanging time string of incoming messages with time of receipt")

if PO_ENABLE and PO_TOKEN != "" and PO_USER != "":
  logPrint("<OK> sending of warnings via Pushover is activated - to disable just set Pushover\PO_ENABLE=False in config")

if useSunCalc:
  if COORD_LAT == "" or COORD_LON == "":
    logPrint("<WARNING> you have to specify Coordinates\LAT and Coordinates\LON in config - falling back to standard calculation of sunhours!")
  else:
    logPrint("<OK> enhanced sunhours calculation with dynamic threshold enabled")

if HIDDEN_FEATURES:
  logPrint("<OK> hidden features enabled - to disable set HIDDEN_FEATURES = False in config")

# v0.09 warn on errors in FWD_REMAP
if fwd_error != "":
  logPrint(fwd_error)                                          # output error in FWD_REMAP

# Webserver und zusaetzlich den UDP-Server starten
myLB_IP = LB_IP if LB_IP != "" else "*"
try:
  server = HTTPServer((LB_IP, int(LBH_PORT)), RequestHandler)
  wst = threading.Thread(target=server.serve_forever)
  wst.daemon = True
  wst.start()
  logPrint("<OK> local http-socket " + myLB_IP + ":" + LBH_PORT + " bound")
  wsconnected = True
except:
  logPrint("<ERROR> can not open http-socket " + myLB_IP + ":" + LBH_PORT)
  wsconnected = False
  pass

if wsconnected:
  #print("LB_IP: "+str(LB_IP)+" LBU_PORT: "+str(LBU_PORT))
  ssock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # Internet, UDP
  try:
    ssock.bind((LB_IP, int(LBU_PORT)))
    sendUDP("SID=" + defSID + " running=1")
    logPrint("<OK> local UDP-socket "+ myLB_IP + ":" +LBU_PORT + " bound")
  except:
    logPrint("<ERROR> can not open UDP-socket " + LBU_PORT + " on ip address " + myLB_IP)
    pass

  logPrint("<OK> remote UDP: " + LOX_IP + ":" + LOX_PORT + " (fragmented max len " + str(UDP_MAXLEN) + ")") if UDP_ENABLE else logPrint("<OK> remote UDP-sending disabled")

  # initial firmware update check
  if UPD_CHECK:
    logPrint("<OK> firmware update check activated with interval " + str(UPD_INTERVAL) + " - to disable set UPD_CHECK = False in config")
    #checkFWUpgrade()
    # in own thread to speed up the start
    t = threading.Thread(target=checkFWUpgrade)
    t.start()
    checkFW = InfiniteTimer(UPD_INTERVAL, checkFWUpgrade)
    checkFW.start()

  # CSV-File oeffnen
  if CSVsave:
    if not os.path.isfile(CSV_NAME):
      try:
        fcsv = open(CSV_NAME,"a+")
        logPrint("<OK> create new CSV-file " + CSV_NAME)
      except:
        logPrint("<ERROR> unable to create CSV-file " + CSV_NAME)
        pass
      if ";" in CSV_FIELDS:
        sep = ";"
      elif "," in CSV_FIELDS:
        sep = ","
      elif " " in CSV_FIELDS:
        sep = " "
      else:
        sep = ";"
      fcsv.write("time" + sep + CSV_FIELDS + "\n")
      fcsv.flush()
    else:
      try:
        fcsv = open(CSV_NAME,"a+")
        logPrint("<OK> open CSV-file " + CSV_NAME)
      except:
        logPrint("<ERROR> unable to open CSV-file " + CSV_NAME)
        pass

  # v0.08 write daily values to CSV-dayfile
  if CSV_DAYFILE != "":
    logPrint("<OK> write daily values to CSV-dayfile "+CSV_DAYFILE)

  # v0.07 create a backup of current config-file
  try:
    os.system("cp -fp " + CONFIG_DIR+"/" + "foshkplugin.conf " + CONFIG_DIR+"/" + "foshkplugin.conf.foshkbackup")
  except:
    pass

  # besser hier den SIGhandler definieren:
  signal.signal(signal.SIGTERM, terminateProcess)
  #for i in [x for x in dir(signal) if x.startswith("SIG")]:
  #  try:
  #    signum = getattr(signal,i)
  #    if signum != 18:
  #      signal.signal(signum,terminateProcess)
  #      if myDebug: logPrint("<DEBUG> SIG-catch enabled for {}".format(i))
  #  except (OSError, RuntimeError, ValueError) as m:
  #    if myDebug: logPrint ("<DEBUG> SIG-catch skipped for {}".format(i))

  while wsconnected:
    try:
      sdata, saddr = ssock.recvfrom(2048)                      # buffer size is 2048 bytes (was: 1024)
      #r_dgram = str(sdata.decode()).strip()
      #r_dgram = str(sdata.decode(encoding='ISO-8859-1',errors='ignore')).strip()
      r_dgram = str(sdata.decode(encoding=OutEncoding,errors='ignore')).strip()
      r_addr = str(saddr[0])
      r_port = str(saddr[1])

      # eingehende Nachricht pruefen
      anzahl = r_dgram.count(',')+1                            # Anzahl der Felder
      if anzahl > 0:
        data=r_dgram.split(",")
        if data[0] == "SID=FOSHKplugin":                       # ist eine zu behandelnde Nachricht
          if data[1] == "System.reboot":                       # reboot-Request
            logPrint("<INFO> reboot request from " + r_addr + " " + sendReboot(WS_IP,WS_PORT))
          elif data[1] == "Plugin.shutdown" and RESTART_ENABLE:  # shutdown-Request - stop FOSHKplugin
            logPrint("<INFO> shutdown request from " + r_addr)
            wsconnected = False
          elif data[1] == "Plugin.getstatus":                  # fragt den aktuellen Status ab
            logPrint("<INFO> getstatus request from " + r_addr)
            # reply current status via sendUDP
            sw_what = " missed=" + SensorIsMissed if inSensorWarning and SensorIsMissed != "" else ""
            sendUDP("SID=" + defSID + " running=" + str(int(wsconnected)) + " wswarning=" + str(int(inWStimeoutWarning)) +  " sensorwarning=" + str(int(inSensorWarning)) + sw_what + " batterywarning=" + str(int(inBatteryWarning)) + " stormwarning=" + str(int(inStormWarning)) + " tswarning=" + str(int(inTSWarning)) + " updatewarning=" + str(int(updateWarning)) + " leakwarning=" + str(int(inLeakageWarning)) + " co2warning=" + str(int(inCO2Warning))  + " intvlwarning=" + str(int(inIntervalWarning)) + " time=" + str(loxTime(time.time())))
          elif data[1] == "Plugin.getminmax":                  # fragt die aktuellen min/max-Werte ab
            logPrint("<INFO> getminmax request from " + r_addr)
            # reply current min/max values via sendUDP
            s = dictToString(min_max," ",False,[],["null"],True,True,True)
            if UDP_MINMAX and s != "": sendUDP("SID=" + defSID + " " + s)
          elif data[1] == "Plugin.debug=enable":               # activate debug mode
            logPrint("<INFO> debug mode via UDP enabled from " + r_addr)
            myDebug = True
          elif data[1] == "Plugin.debug=disable":              # disable debug mode
            logPrint("<INFO> debug mode via UDP disabled from " + r_addr)
            myDebug = False
          elif data[1] == "Plugin.pushover=enable":            # activate Pushover warnings
            if PO_USER != "" and PO_TOKEN != "":
              PO_ENABLE = True
              logPrint("<INFO> pushover warning via UDP enabled from " + r_addr)
            else:
              logPrint("<INFO> pushover warning could not be activated via UDP from " + r_addr + " - USER or TOKEN are not correctly set in config")
          elif data[1] == "Plugin.pushover=disable":           # disable Pushover warnings
            logPrint("<INFO> pushover warning via UDP disabled from " + r_addr)
            PO_ENABLE = False
          elif data[1] == "Plugin.leakwarning=enable":         # activate leak warning
            logPrint("<INFO> leakwarning via UDP enabled from " + r_addr)
            LEAKAGE_WARNING = True
          elif data[1] == "Plugin.leakwarning=disable":        # disable leak warning
            logPrint("<INFO> leakwarning via UDP disabled from " + r_addr)
            LEAKAGE_WARNING = False
          elif data[1] == "Plugin.co2warning=enable":          # activate co2 warning
            logPrint("<INFO> co2warning via UDP enabled from " + r_addr)
            CO2_WARNING = True
          elif data[1] == "Plugin.co2warning=disable":         # disable co2 warning
            logPrint("<INFO> co2warning via UDP disabled from " + r_addr)
            CO2_WARNING = False
          elif data[1] == "Plugin.intvlwarning=enable":        # activate interval warning
            logPrint("<INFO> intvlwarning via UDP enabled from " + r_addr)
            CO2_WARNING = True
          elif data[1] == "Plugin.intvlwarning=disable":       # disable interval warning
            logPrint("<INFO> intvlwarning via UDP disabled from " + r_addr)
            CO2_WARNING = False
    except:
      if sndlog :
        doNothing()
        #sndPrint("<WARNING> except in while wsconnected! (" + str(sys.exc_info()[0]) + ")",True)
      break
    # testweise mal raus und oberhalb der while-Schleife
    #signal.signal(signal.SIGTERM, terminateProcess)
  try:
    wst.do_run = False
    ssock.close()
  except:
    pass

  logPrint("<OK> local UDP-socket " + myLB_IP + ":" + LBU_PORT + " closed")
  if CSVsave:
    try:
      fcsv.close()
      logPrint("<OK> close CSV-file " + CSV_NAME)
    except:
      logPrint("<ERROR> unable to close CSV-file " + CSV_NAME)
      pass

# InfiniteTimer wieder stoppen
if WSDOG_WARNING: checkWS.cancel()
if UPD_CHECK:
  try:
    checkFW.cancel()
  except:
    pass

# definiert herunterfahren
if STORM_WARNING: savePickle(CONFIG_FILE, CONFIG_DIR+"/"+prgname+"-"+LBH_PORT+"-stundenwerte.pkl")
saveMinMax(min_max_pickle)
sendUDP("SID=" + defSID + " running=0")
allPrint("<OK> "+prgname+" "+prgver+" stopped")
#terminateProcess(0,0)

