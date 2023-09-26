# Fully Autonomous Omnivore 🤖

*A script to convert All In Energy's vendor data into a All In Energy's salesforce format.**

## Features

* Converts All In Energy's vendor data into a All In Energy's salesforce format.
* Uses PDM to test and build.
* Easy to use, even for beginners! 

## 📥 Installation

To develop this script, you will need to have [PDM](https://pdm.fming.dev) installed. PDM is a modern Python package manager and development toolkit.

```
pip install pdm
```

## 💾 Usage

1. Clone this repository

```
git clone https://github.com/yourname/fully-autonomous-omnivore.git
```

2. Install dependencies  

```
pdm install
```

3. Create a .env file with your credentials like:

```
EMAIL=test@allinenergy.org.test
CONSUMER_KEY=alksjnfdlkjfnsdkjfnkjsdnfkajnsdfasdf
PRIVATEKEY_FILE=key.pem
ENV=test
```

4. Run the script
```
pdm start
```

## 🧪 Testing

### Unit Test

```
pdm run test
```

### Staging Test

Create a .env file with your credentials like:

```
EMAIL=test@allinenergy.org.test
CONSUMER_KEY=alksjnfdlkjfnsdkjfnkjsdnfkajnsdfasdf
PRIVATEKEY_FILE=key.pem
ENV=staging
```

```
pdm run staging
```
