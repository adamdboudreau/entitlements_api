FROM ruby:2.1

ENV \
    RAKE_ENV=dev \
    PUMA_WORKERS=1 \
    PUMA_MIN_THREADS=4 \
    PUMA_MAX_THREADS=16

RUN mkdir -p /usr/src/app

COPY . /usr/src/app

RUN mkdir /usr/src/app/logs

WORKDIR /usr/src/app

RUN bundle install --jobs 20 --retry 5

EXPOSE 9292

CMD ["puma","-C","puma.rb"]

# after build
# rake create RACK_ENV=dev
# rake migrate RACK_ENV=dev