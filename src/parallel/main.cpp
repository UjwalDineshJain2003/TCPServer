/*
 * tcpserver.c - A multithreaded TCP echo server
 * usage: tcpserver <port>
 *
 * Testing :
 * nc localhost <port> < input.txt
 */

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <cstring>
#include <pthread.h>
#include <iostream>
#include <map>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sstream>

#define MAX_THREADS 5

std::map<std::string, std::string> KV_DATASTORE;

void *client_handler(void *arg);

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

    int parallel_server_socket, client_socket;
    // Server address creation for binding
    struct sockaddr_in parallel_server_address; 
    // Client address variable to store information of address of client
    struct sockaddr_in client_address;
    socklen_t client_address_len = sizeof(client_address);

    // creating a socket
    parallel_server_socket = socket(AF_INET, SOCK_STREAM, 0);
    
    if (parallel_server_socket < 0)
    {
        perror("ERROR opening socket");
        exit(1);
    }

    // Initialize server address structure
    memset(&parallel_server_address, 0, sizeof(parallel_server_address));
    parallel_server_address.sin_family = AF_INET;
    // binding to any available IP address in the machine
    parallel_server_address.sin_addr.s_addr = INADDR_ANY;
    // converting (host) port number to network byte order, as network data is in big endian, while system data is in little endian format
    parallel_server_address.sin_port = htons(portno);

    // binding the socket to an address
    if (bind(parallel_server_socket, (struct sockaddr *)&parallel_server_address, sizeof(parallel_server_address)) < 0)
    {
        perror("Could not bind socket! Error!");
        exit(1);
    }

    // listening with a defined queue length (multiple requests must wait, because they're on the same port, so we define a max number of requests that can be accomodated for waiting, beyond which requests will just be dropped)
    listen(parallel_server_socket, 5);

    // defining the array of threads in order to deal with multiple clients at the same time
    pthread_t threads[MAX_THREADS];
    int thread_array_index = 0;

    while (1)
    {
        // accepting any incoming connections from the client side
        client_socket = accept(parallel_server_socket, (struct sockaddr *)&client_address, &client_address_len);
        
        // error in getting client socket
        if (client_socket < 0)
        {
            perror("Unable to accept client connection! Error!");
            exit(1);
        }

        // creating a new pthread and dealing with any error in creating it
        if (pthread_create(&threads[thread_array_index++ % MAX_THREADS], NULL, client_handler, (void *)&client_socket) != 0)
        {
            perror("Unable to create thread! Error!");
            exit(1);
        }
    }
    
    // closing the server socket
    close(parallel_server_socket);
    return 0;
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
                close(client_socket);
                break;
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
