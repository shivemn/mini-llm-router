FROM node:20-alpine

ENV NODE_ENV=production
ENV HOST=0.0.0.0
ENV PORT=3001

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY --chown=node:node server.mjs instrumentation.mjs ./

USER node
EXPOSE 3001

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:3001/health/live').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

CMD ["node", "--experimental-loader=@opentelemetry/instrumentation/hook.mjs", "--import", "./instrumentation.mjs", "server.mjs"]
