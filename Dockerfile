# Use Node.js version 20 with Alpine as the base image
FROM node:20-alpine

# Set the working directory in the container
WORKDIR /app

# Copy package.json and yarn.lock to the container
COPY package.json yarn.lock ./

# Install dependencies
RUN yarn install

# Copy all source files to the container
COPY . .


# Expose the port (if required by the application)
EXPOSE 3005


# Run the backfill and stream concurrently
CMD ["sh", "-c", "yarn backfill & yarn stream"]
