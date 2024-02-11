/*
 * tcpserver.c - A multithreaded TCP echo server
 * usage: tcpserver <port>
 *
 * Testing :
 * nc localhost <port> < input.txt
 */

#include <stdio.h>
#include <iostream>
#include <unistd.h>
#include <stdlib.h>
#include <cstdlib>
#include <string>
#include <cstring>
#include <pthread.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <map>
#include <sstream>
#include <vector>
// creating the key value store
std::map<std::string, std::string> KV_DATASTORE;

// creating the mutex lock for the key value store
void *client_handler(void *);

std::vector<std::string> split(std::string s, char c)
{

    std::vector<std::string> split_string;
    std::string initial = s.substr(0, s.find(c));
    std::string rest = s.substr(s.find(c) + 1);
    do
    {
        split_string.push_back(initial);
        rest = rest.substr(rest.find(c) + 1);
    } while (rest != "");

    return split_string;
}

int main(int argc, char **argv)
{

    int portno; /* port to listen on */

    /*
     * check command line arguments
     */
    if (argc != 2)
    {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }

    // DONE: Server port number taken as command line argument
    portno = atoi(argv[1]);

    // creating a socket
    int serial_server_socket = socket(AF_INET, SOCK_STREAM, 0);
    // Server address creation for binding
    struct sockaddr_in serial_server_address;
    serial_server_address.sin_family = AF_INET;
    // binding to any available IP address in the machine
    serial_server_address.sin_addr.s_addr = INADDR_ANY;
    // converting (host) port number to network byte order, as network data is in big endian, while system data is in little endian format
    serial_server_address.sin_port = htons(portno);

    // binding the socket to an address
    if (bind(serial_server_socket, (struct sockaddr *)&serial_server_address, sizeof(serial_server_address)) < 0)
    {
        perror("Could not bind socket! Error!");
        exit(1);
    }
    // listening with a defined queue length (multiple requests must wait, because they're on the same port, so we define a max number of requests that can be accomodated for waiting, beyond which requests will just be dropped)
    listen(serial_server_socket, 5);
    while (1)
    {
        // Client address variable to store information of address of client
        struct sockaddr_in client_address;
        socklen_t client_len = sizeof(client_address);

        // accepting any incoming connections from the client side
        int client_socket = accept(serial_server_socket, (struct sockaddr *)&client_address, &client_len);
        // creating a thread to handle client requests
        pthread_t tid;
        pthread_create(&tid, NULL, client_handler, (void *)&client_socket);
    }

    close(serial_server_socket);
    pthread_exit(NULL);
}

void *client_handler(void *arg)
{
    int client_socket = *((int *)arg);
    char buffer[2048];
    ssize_t bytes_received;

    while ((bytes_received = recv(client_socket, buffer, sizeof(buffer), 0)) > 0)
    {
        // adding to indicate an end
        buffer[bytes_received] = '\0';
        std::string request(buffer);

        // splitting the request into individual commands
        std::istringstream iss(request);
        std::string command;
        while (std::getline(iss, command, '\n'))
        {
            // initialising the response to be sent to the client
            std::string response;

            if (command == "READ")
            {
                // Read the key (till the newline character)
                std::getline(iss, command, '\n');
                std::string key = command;
                if (KV_DATASTORE.find(key) != KV_DATASTORE.end())
                {
                    // a key with the specified value was FOUND
                    response = KV_DATASTORE[key] + "\n";
                }
                else
                {
                    // the key doesnt exist
                    response = "NULL\n";
                }
            }
            else if (command == "WRITE")
            {
                // Read the key and value 
                std::string key, value;
                std::getline(iss, key, '\n');
                std::getline(iss, value, '\n');
                // getting rid of ':' delimiter
                value.erase(0, 1);
                // storing into the datastore
                KV_DATASTORE[key] = value;
                response = "FIN\n";
            }
            else if (command == "COUNT")
            {
                // return the response with a newline character attached
                response = std::to_string(KV_DATASTORE.size()) + "\n";
            }
            else if (command == "DELETE")
            {
                // Read the key
                std::getline(iss, command, '\n');
                std::string key = command;
                if (KV_DATASTORE.find(key) != KV_DATASTORE.end())
                {
                    // key found, delete value
                    KV_DATASTORE.erase(key);
                    response = "FIN\n";
                }
                else
                {
                    // key not found
                    response = "NULL\n";
                }
            }
            else if (command == "END")
            {
                // End the connection
                send(client_socket, "\n", 1, 0);
                close(client_socket);
            }
            else
            {
                // not a valid command typed
                response = "INVALID COMMAND\n";
            }

            send(client_socket, response.c_str(), response.size(), 0);
        }
    }

    close(client_socket);
    pthread_exit(NULL);
}
