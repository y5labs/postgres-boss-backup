FROM node:18-alpine
RUN apk add --no-cache postgresql-client
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY ./package.json /usr/src/app/
RUN npm install && npm cache clean --force
COPY ./ /usr/src/app
ENV NODE_ENV production
CMD ["npm", "start"]