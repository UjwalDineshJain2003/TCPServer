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
    struct sockaddr_in server_address, client_address;
    socklen_t client_address_len = sizeof(client_address);

    // Create socket
    parallel_server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (parallel_server_socket < 0)
    {
        perror("ERROR opening socket");
        exit(1);
    }

    // Initialize server address structure
    bzero((char *)&server_address, sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(portno);

    // Bind the socket to the address
    if (bind(parallel_server_socket, (struct sockaddr *)&server_address, sizeof(server_address)) < 0)
    {
        perror("ERROR on binding");
        exit(1);
    }

    // Start listening for connections
    listen(parallel_server_socket, 5);

    pthread_t threads[MAX_THREADS];
    int thread_index = 0;

    while (1)
    {
        client_socket = accept(parallel_server_socket, (struct sockaddr *)&client_address, &client_address_len);
        if (client_socket < 0)
        {
            perror("ERROR on accept");
            exit(1);
        }

        if (pthread_create(&threads[thread_index++ % MAX_THREADS], NULL, client_handler, (void *)&client_socket) != 0)
        {
            perror("ERROR creating thread");
            exit(1);
        }
    }

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
        buffer[bytes_received] = '\0';
        std::string request(buffer);

        // Split the request into individual commands
        std::istringstream iss(request);
        std::string command;
        while (std::getline(iss, command, '\n'))
        {
            std::string response;

            if (command == "READ")
            {
                // Read the key
                std::getline(iss, command, '\n');
                std::string key = command;
                if (KV_DATASTORE.find(key) != KV_DATASTORE.end())
                {
                    response = KV_DATASTORE[key] + "\n";
                }
                else
                {
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
                KV_DATASTORE[key] = value;
                response = "FIN\n";
            }
            else if (command == "COUNT")
            {
                response = std::to_string(KV_DATASTORE.size()) + "\n";
            }
            else if (command == "DELETE")
            {
                // Read the key
                std::getline(iss, command, '\n');
                std::string key = command;
                if (KV_DATASTORE.find(key) != KV_DATASTORE.end())
                {
                    KV_DATASTORE.erase(key);
                    response = "FIN\n";
                }
                else
                {
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
                response = "INVALID COMMAND\n";
            }

            send(client_socket, response.c_str(), response.size(), 0);
        }
    }

    close(client_socket);
    pthread_exit(NULL);
}
