Instructions for deploying the project:

========================SSL Key Pair========================

The service directories:

	gateway
	administrator
	oauth_interface

need to contain an SSL key pair with 'cert.pem' file storing the certificate and 'key.pem' storing the private key.
If a self-signed certificate will be used, the 'cert.pem' file needs to be delivered to all clients.

========================Registering a Google OAuth Client========================

The system is registered as a Google OAuth client on the following web address: https://console.developers.google.com.
The user has to be a registered Google user in order to access this web address.
After creating a new project, on the Credentials page the name of the client is set up and the Client Id and Client Secret is
obtained. These values have to be included in the the 'oauth_interface' directory of our project, under the 
'google_client_secret.json' file, following the structure given bellow:

{
	"client_id": "############################",
	"client_secret": "########################"
}

========================OAuth redirect URI========================

The URI that will be used inside the OAuth Interface service needs to be registered at the previously mentioned Credentials
page. For local deployement, the address https://127.0.0.1:#### should be used with the port being customly chosen by the
user. This address and this port need to be included in the 'docker-compose.yml' file, in the 'oauth' service with it looking like described bellow:

oauth:
    build: ./oauth_interface
    ports:
      - "####:5000"
    volumes:
      - ./oauth:/usr/src/app
    environment:
      AMQP_URL: rabbitmq
      QUEUE: oauth-service
      REDIRECT_URI: "https://127.0.0.1:####/"
    depends_on:
      - "rabbitmq"

with fields '####' being replaced by the chosen port. When deploying the services localy, we need to make sure that the chosen port is being port-forwarded from the Virtual Machine on which the Docker is running to our local machine, so that Google can access the Virtual Machine from our local machine.

========================Deploying the system locally========================

In order to deploy the system locally, we need to have Docker installed and running on the Virtual Machine that we have chosen.
We also need to install a Docker Compose section, which can be done by running the 'pip install docker-compose' command, if we
previously installed pip. Then, the 'docker-compose.yml' is used by the Docker Compose to build the services into images and 
run them in containers by simply running the command 'docker-compose up'.