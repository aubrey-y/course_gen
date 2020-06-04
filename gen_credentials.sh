credentials=$1

sudo gem install travis

mv "$credentials" client-secret.json

travis login --org

travis encrypt-file credentials.tar.gz --org -r aubrey-y/course_gen
