
FROM node:20-alpine

# Set the working directory in the container
WORKDIR /app

# Copy package.json and package-lock.json to the container
COPY package.json yarn.lock ./

# Install dependencies
RUN yarn install

# Copy all source files to the container
COPY . .

# Set environment variables (replace with actual values or set during runtime)
ENV DATABASE_URL=postgresql://indexer:password@postgress:5432/indexer
ENV REDIS_URL=redis://localhost:6379
ENV HUB_REST_URL=https://hub.pinata.cloud
ENV HUB_RPC=hub-grpc.pinata.cloud
ENV HUB_SSL=true
ENV WORKER_CONCURRENCY=5
ENV LOG_LEVEL=debug

# Expose port 
EXPOSE 3005
# Run the backfill process
RUN yarn migrate 

# Run the indexer (can be modified depending on the process)
CMD ["sh", "-c", "yarn backfill & yarn stream"]