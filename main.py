from classes.node import Node
import sys

my_ip = sys.argv[1]
my_port = int(sys.argv[2])

node = None
created = False
joined = False
while True:
    print('1. Create')
    print('2. Join')
    print('3. Print Finger Table')
    print('4. Print Hash')
    print('5. Store')
    print('6. Download')
    print('7. Print Stored Files')
    choice = input('Enter command number to proceed:\n')

    if choice == '1' and not created and not joined:
        print("Creating")
        node = Node.create(my_ip, int(my_port))
        created = True
        print("Created")
    elif choice == '2' and not joined and not created:
        print("Joining")
        # n_ip = input('Enter IP of existing node in ring:\n')
        n_port = input('Enter port of existing node in ring:\n')
        node = Node.join(my_ip, my_port, "192.168.100.3", n_port)
        joined = True
        print("Joined")
    elif choice == '3':
        if node:
            for fnode in node.finger_table:
                print("Hash {0}", fnode.id)
    elif choice == '4':
        if node:
            print('Hash: {0}', node.id)
    elif choice == '5':
        # filename = input('Enter file name to store \n')
        filename = 'image.jpg'
        file_to_store = open(filename, 'rb')
        value_to_store = file_to_store.read()
        file_to_store.close()
        node.store_file(filename,value_to_store)

    elif choice == '6':
        filename_to_download = input('Enter file name to download \n')
        file_value = node.download_file(filename_to_download)
        if file_value and "filedata" in file_value:
            download_file = open(filename_to_download, 'wb')
            download_file.write(file_value["filedata"])
            download_file.close()
        else:
            print("Not able to Download file, sorry!")

    elif choice == '7':
        print(node.files)


