# Dockerfile
# Build Node app
FROM node:20-alpine

# Create app directory
WORKDIR /usr/src/app

# Install build deps for better-sqlite3 (optional) and others
RUN apk add --no-cache python3 make g++ sqlite sqlite-dev

# Copy package.json and package-lock if any
COPY package.json .

# Install dependencies
RUN npm install --production

# Copy app source
COPY . .

# Expose port
EXPOSE 4000

# Start the app
CMD ["node", "server.js"]
