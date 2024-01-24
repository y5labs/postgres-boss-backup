FROM node:18-alpine
RUN apk add --no-cache postgresql-client
# add the b2 self contained cli for backblaze bucket io
RUN wget https://github.com/Backblaze/B2_Command_Line_Tool/releases/latest/download/b2-linux 
RUN mv b2-linux /usr/local/bin/b2 
RUN sudo chmod +x /usr/local/bin/b2
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY ./package.json /usr/src/app/
RUN npm install && npm cache clean --force
COPY ./ /usr/src/app
ENV NODE_ENV production
CMD ["npm", "start"]