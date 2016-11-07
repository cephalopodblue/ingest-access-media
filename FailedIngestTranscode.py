import struct
import yaml
import datetime
import os
import collections

FormatInfo = collections.namedtuple("FormatInfo", "FormatCode Channels Samplerate Bitrate")

def get_format_information(src_file):
    # chunk header has form
    # FORMAT: 4 bytes ascii SIZE: 4 bytes integer
    chunk_header = "4si"
    header_size = 8

    # Format chunk - required for WAV files - we aren't actually interested in the extended format info,
    # so decode first 16 bytes & just dump the rest in back
    std_format_size = 16

    # get format metadata
    with open(src_file, "rb") as f:
        riff_chunk = struct.unpack(chunk_header, f.read(header_size))
        fmat = f.read(4).decode('ascii')
        # WAV format chunk : audio format code 2 byte short - number of channels 2 byte short -
        #       sample rate 4 byte int - byte rate 4 byte int - block alignment 2 byte short -
        #       bits per sample 2 byte short - [optional: extended wav info size (can and often is zero) - 
        #       extended wav information of size in previous section]
        format_header = struct.unpack(chunk_header, f.read(header_size))
        format_structure = "hhiihh" + str(format_header[1] - std_format_size) + "s"
        format_body = struct.unpack(format_structure, f.read(format_header[1]))
        
        format_information = FormatInfo(format_body[0], format_body[1], format_body[2], format_body[3] * 8)
        return format_information
        
if __name__ == "__main__":
    with open("config.yml") as f:
        config = yaml.load(f)
        
    today = datetime.date.today()
    log_file = os.path.join(config["staging"], today.strftime("transcode_log_%m-%d-%Y.txt"))
    
    with open(config["transcode_bat"], "w+") as f:
        print("Writing transcode batch file")
        
    with open(log_file, "w+") as f:
        f.write("Itemcode\t")
        for field in FormatInfo._fields:
            f.write(field + "\t")
        f.write("\n")
        
    for file_name in os.listdir(config["failure_directory"]):
        item_code = os.path.splitext(file_name)[0]
        # get failed directory from configurations
        full_path = os.path.join(config["failure_directory"], file_name)
        staging_path = os.path.join(config["staging"], file_name)
        dest_path = os.path.join(config["media_ingest"], file_name)
        format_info = get_format_information(full_path)
        with open(log_file, "a") as f:
            f.write(item_code)
            for field in format_info:
                f.write(str(field) + "\t")
            f.write("\n")
        with open(config["transcode_bat"], "a") as f:
            f.write(config["ffmpeg_location"] + " -i \"" + full_path + "\" " + config["ffmpeg_command"] + " \"" + staging_path + "\"\n")
            f.write("del /f \"" + full_path + "\"\n")
            f.write("move \"" + staging_path + "\" \"" + dest_path + "\"\n")
