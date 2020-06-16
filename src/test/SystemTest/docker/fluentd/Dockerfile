FROM fluent/fluentd:v1.10-1

USER root
RUN mkdir -p /var/log/td-agent/buffer && chmod 777 /var/log/td-agent/buffer

RUN apk add --no-cache --update --virtual .build-deps \
        sudo build-base ruby-dev \
 # cutomize following instruction as you wish
 && sudo gem install fluent-plugin-kafka \
 && sudo gem sources --clear-all \
 && apk del .build-deps \
 && rm -rf /home/fluent/.gem/ruby/2.5.0/cache/*.gem

COPY fluent.conf /fluentd/etc/

USER fluent
