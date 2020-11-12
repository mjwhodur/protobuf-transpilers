import argparse

parser = argparse.ArgumentParser(description='Process LOAD DATA INFILE to Protobuf')
parser.add_argument('file', metavar='FILE', type=str,
                    help='File to process')
parser.add_argument('--strip_args', metavar="arg", type=str, help="Strip occurences of a string")

args = parser.parse_args()

with open(args.file, 'r') as f:
    lines = f.readlines()
    create_msgs = False
    print("message {} ".format(args.file.split('.')[0]) + " {")
    counter = 1
    for line in lines:
       if len(line) > 4 and create_msgs:
            data = line.strip().split(' ')
            msg_type = data[1]
            field = data[0]
            optional = False
            if 'NULL' in data[3]:
                optional = True

            field = field.strip('"').replace("+", "_")
            if args.strip_args:
                field = field.replace(args.strip_args, "")

            if "UINT" in msg_type:
                if "64" in msg_type:
                    msg_type = "uint64"
                else:
                    msg_type = "uint32"

            if "CHAR" in msg_type:
                msg_type = "string"
            if "TIMESTAMP" in msg_type:
                msg_type = "string"
            if "TIME" in msg_type:
                msg_type = "string"
            if "DATE" in msg_type:
                msg_type = "string"
            if "DATA" in msg_type:
                msg_type = "bytes"

            if optional:
                print(f"optional {msg_type} {field}  = {counter};")
            counter = counter + 1
       else:
           if "(" in line:
               create_msgs = True
           elif ")" in line:
               create_msgs = False

    print("}")
