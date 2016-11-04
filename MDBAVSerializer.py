import os
import re
import xml.etree.ElementTree as et
import uuid
import shutil
import datetime
import unicodedata
import collections

import yaml
from yattag import Doc, indent
import pypyodbc


TITLES_PER_FILE = 500
forbidden_unicode = 'Cc'

ingested_meta_pattern = re.compile("(audiovault_metadata(_(\d){8})?|dalet_audiovault)_\d+.xml_DONE", flags=re.I)

required_meta = ["Description", "Cat", "ClientID", "StartDate", "KillDate", "OutCue", 
                  "Codes", "Space", "DefaultDuration", "Vault", "Shared", "SampleRate", 
                  "Hidden", "AudioLength", "TLM_Body", "Tracks", "DriveID", "FormatName", 
                  "AudioEnd", "Path", "Class", "TLA_Body", "FileType", "TLA_Header", 
                  "Found", "TLM_Header", "CompName", "UFN", "File", "AudioBegin"]
AVMeta = collections.namedtuple("AVMeta", required_meta)


class AudiovaultSerializer:

    source = "Audio Vault"

    def __init__(self, staging, ingest, media, media_ingest):
        self.staging = staging
        self.ingest = ingest
        self.media = media
        
        now = datetime.date.today()
        
        self.success_log_file = os.path.join(self.staging, now.strftime("success_log_%d-%m-%y.txt"))
        self.failed_log_file = os.path.join(self.staging, now.strftime("failed_log_%d-%m-%y.txt"))
        self.media_staging = os.path.join(self.staging, "media")
        if not os.path.exists(self.media_staging):
            os.makedirs(self.media_staging)
        
        print("\tDalet XMLs at: " + self.staging)
        with open(self.success_log_file, "w") as f:
            print("\tSuccess log at: " + self.success_log_file)
        with open(self.failed_log_file, "w") as f:
            print("\tFailed log at: " + self.failed_log_file)
        print("\tMedia staging at: " + self.media_staging)
        
        self.media = media
        self.media_ingest = media_ingest
        if self.media_ingest:
            print("\tCopying media to: " + self.media_ingest)
                
        self.itemcode_mapping = {}
        self.to_copy = {}
        
        # Get path -> Itemcode mapping of old audiovault ingests to prevent duplicate titles
        for meta_file in os.listdir(ingest):
            if ingested_meta_pattern.match(meta_file):
                # this metadata is an old XML - get past filepath - itemcode mapping
                # so that we don't accidentally create duplicate titles
                tree = et.parse(os.path.join(ingest, meta_file))
                root = tree.getroot()
                for child in root:
                    media_path = child.find("AV_Path").text
                    category = child.find("AV_Cat").text
                    if category:
                        location = os.path.join(category, os.path.split(media_path)[1])
                    else:
                        location = os.path.split(media_path)[1]
                    try:
                        self.itemcode_mapping[media_path] = child.find("Itemcode").text
                    except AttributeError:
                        try:
                            self.itemcode_mapping[media_path] = child.find("ItemCode").text
                        except AttributeError:
                            print(child.find("Itemcode"))
                            print(media_path)
                            print(category)
                            print(meta_file)
        print("\tGenerating \'KEXPAllegianceMediaRef\' from audiovault Description and File")
        
    def get_itemcode(self, avmeta):
        # if we don't have actual media corresponding to this metadata, don't serialize it
        media_path = str(avmeta.Path)
        
        media_location = os.path.join(str(avmeta.Cat), os.path.basename(str(avmeta.Path)))
        item_code = None
        
        
        if not os.path.exists(os.path.join(self.media, media_location)):
            # don't have any media for this item; don't serialize it
            media_location = None
            
        # If media is already a title in dalet, re-use the previous itemcode (prevent duplicate titles)
        if media_path in self.itemcode_mapping.keys():
            item_code = self.itemcode_mapping[media_path]
        else:
            item_code = str(uuid.uuid4())
            
        return item_code, media_location


    def serialize(self, access_db, from_date):
        # Serialize audiovault metadata XMLS from Access DB file ("AVAir")
        conn = pypyodbc.win_connect_mdb(access_db)
        cursor = conn.cursor()
        query = "SELECT " + ", ".join(required_meta) + " FROM Files WHERE TLM_Header > " + from_date.strftime("#%m/%d/%Y#")
        updated_meta = cursor.execute(query)
        
        results = updated_meta.fetchmany(TITLES_PER_FILE)
        serialized = 0

        xml_files = []
        titles = []
        while(len(results) > 0):
            for row in results:
                avmeta = AVMeta(*row)
                item_code, media_location = self.get_itemcode(avmeta)
            
                # if we have media (and an itemcode), perform serialization
                if item_code and media_location:
                  self.to_copy[os.path.normcase(os.path.join(self.media, media_location))] = item_code
                  titles.append(avmeta)
                  
            results = updated_meta.fetchmany(TITLES_PER_FILE)
        cursor.close()
        conn.close()
        
        sliced = titles[0:TITLES_PER_FILE]
        while len(sliced) > 0:
            doc, tag, text = Doc().tagtext()
            doc.asis('<?xml version="1.0" encoding="UTF-8"?>')
            with tag("titles"):
                for avmeta in sliced:
                    allegiance = ""
                    file_name = "".join([c for c in avmeta.File if unicodedata.category(c) not in forbidden_unicode])

                    allegiance = str(avmeta.Description) + "/" + file_name

                    with tag("title"):
                        with tag("Itemcode"):
                            text(item_code)
                        with tag("Key1"):
                            text(item_code)
                        with tag("TitleName"):
                            meta_value = str(avmeta.Description)
                            text(meta_value)
                        with tag("KEXPAllegianceMediaRef"):
                            text(allegiance)
                        with tag("KEXPAudioVaultCategory"):
                            meta_value = str(avmeta.Cat)
                            text(meta_value)
                        with tag("KEXPClient"):
                            meta_value = str(avmeta.ClientID)
                            text(meta_value)
                        with tag("KEXPStartDate"):
                            meta_value = avmeta.StartDate
                            if meta_value:
                                meta_value = meta_value.strftime('%m/%d/%Y %I:%M:%S')
                            else:
                                meta_value = str(meta_value)
                            text(meta_value)
                        with tag("TitleKillDate"):
                            meta_value = avmeta.KillDate
                            if meta_value:
                                meta_value = meta_value.strftime('%m/%d/%Y %I:%M:%S')
                            else:
                                meta_value = str(meta_value)
                            text(meta_value)
                        with tag("KEXPOutCue"):
                           meta_value = str(avmeta.OutCue)
                           text(meta_value)

                        with tag("KEXPSource"):
                            text(self.source)

                            
                        for meta_name, meta_value in avmeta._asdict().items():
                            dalet_name = "AV_" + meta_name
                            if type(meta_value) == datetime.datetime:
                                meta_value = meta_value.strftime('%m/%d/%Y %I:%M:%S')
                            else:
                                meta_value = str(meta_value)
                            with tag(dalet_name):
                                text(meta_value)
                                
                        with open(self.success_log_file, "a+") as log:
                            log.write(item_code + "\t" + str(media_location) + "\t" + str(avmeta.Path) + "\n")
                        serialized += 1
            
            formatted_data = indent(doc.getvalue())
            output_file = "audiovault_metadata_" + from_date.strftime("%m%d%Y_") + str(len(xml_files)) + ".xml"
            output_file = os.path.join(self.staging, output_file)
            with open(output_file, "wb") as f:
                f.write(formatted_data.encode("UTF-8"))    
            print(str(serialized) + " Audiovault title XMLs written")
            xml_files.append(output_file)
            end_slice = serialized + TITLES_PER_FILE
            sliced = titles[serialized:end_slice]
            break
        print(serialized)
        print(end_slice)
        
        most_recent = 0
        print(self.to_copy)
        for directory in os.listdir(self.media):
            d = os.path.join(self.media, directory)
            for file_name in os.listdir(d):
                full_name = os.path.normcase(os.path.join(d, file_name))
                last_modified = os.path.getmtime(full_name)
                if last_modified > most_recent:
                    most_recent = last_modified
                if full_name not in self.to_copy:
                    with open(self.failed_log_file, "a+") as f:
                        f.write(os.path.join(directory, file_name) + "\n")
                        
        move_meta = input("\tMove XMLs to ingest directory? ")
        if move_meta.casefold().startswith("y"):
            for f in xml_files:
                shutil.copy(f, self.ingest + "\\")
                
            if self.media_ingest:
                move_media = input("\tMove media files to media ingest directory? ")
                if move_media.casefold().startswith("y"):
                    for input_file, item_code in self.to_copy.items():
                        output_file = os.path.join(self.media_ingest, item_code + os.path.splitext(input_file)[1])
                        print("Copying " + str(input_file) + " to " + str(output_file))
                        shutil.copy(input_file, output_file)
                          
        return datetime.date.fromtimestamp(most_recent)
        
if __name__ == "__main__":
    config_file = "config.yml"
    
    # load configuration settings
    config = None
    with open(config_file) as f:
        config = yaml.load(f)

    from_date = datetime.datetime(*config['last_ingest'])
    
    serializer = AudiovaultSerializer(config['staging'], config['meta_ingest'], config['media_files'], config['media_ingest'])
    last_modified = serializer.serialize(config['access_db'], from_date)
    if last_modified:
        with open(config['copy_bat'], 'w+') as f:
            f.write("xcopy " + config["av_media"] + " " + config["media_files"] + " /d:" + last_modified.strftime("%m-%d-%Y") + " /i /s\n\n")
            f.write("xcopy " + config["av_location"] + " .\\")