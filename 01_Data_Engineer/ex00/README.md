# Create PostGreSQL DataBase with Docker-Compose

- change password set in the .env with the one suit you
- Run the docker-compose.yl with : docker-compose up --build -d
- to run bash inside container : docker exec -it <container_name> bash
- to access to your databse : psql -U asalek -d piscineds


git change username and email:
    git config --global --edit
git change commit and commiter username and commiter email
    git commit --amend --reset-author
git set username :
    git config --global user.name "Your Name"
git set email :
    git config --global user.email you@example.com