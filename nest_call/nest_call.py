
import requests
import configparser




# generate a login URL, the code is generated using thsi URL. 
def generate_url():
    url = 'https://nestservices.google.com/partnerconnections/'+project_id+'/auth?redirect_uri='+redirect_uri+'&access_type=offline&prompt=consent&client_id='+client_id+'&response_type=code&scope=https://www.googleapis.com/auth/sdm.service'
    #print("Go to this URL to log in:")
    return(url)
       

## with the code we can generate access token and refresh token. 
## The access token is only valid for 60 minutes. You can use the refresh token to renew it.
# Refresh token
def refresh_token(refresh_token):

   
        params = (
            ('client_id', client_id),
            ('client_secret', client_secret),
            ('refresh_token', refresh_token),
            ('grant_type', 'refresh_token'),
            )

        response = requests.post('https://www.googleapis.com/oauth2/v4/token', params=params)

        response_json = response.json()
        
        new_access_token = response_json['token_type'] + ' ' + response_json['access_token']
        print('New Access token: ' + new_access_token)
        return new_access_token
   




# Get structures

def get_structure(access_token, project_id):

    url_structures = 'https://smartdevicemanagement.googleapis.com/v1/enterprises/' + project_id + '/structures'

    headers = {
        'Content-Type': 'application/json',
        'Authorization': access_token, 
    }
    response = requests.get(url_structures, headers=headers)
    print(response.json())
    return(response.json())
    



# Get device name
def device_name(access_token, project_id):
    url_get_devices = 'https://smartdevicemanagement.googleapis.com/v1/enterprises/' + project_id + '/devices'

    headers = {
        'Content-Type': 'application/json',
        'Authorization': access_token,
    }

    response = requests.get(url_get_devices, headers=headers)
    response_json = response.json()
    device_0_name = response_json['devices'][0]['name']
    
    return(device_0_name)


# Get device stats
def device_status_humidity(access_token, device_name):

    url_get_device = 'https://smartdevicemanagement.googleapis.com/v1/' + device_name

    headers = {
        'Content-Type': 'application/json',
        'Authorization': access_token,
    }

    response = requests.get(url_get_device, headers=headers)
    response_json = response.json()

    humidity = response_json['traits']['sdm.devices.traits.Humidity']['ambientHumidityPercent']
    return(humidity)

def device_status_temperature(access_token, device_name):

    url_get_device = 'https://smartdevicemanagement.googleapis.com/v1/' + device_name

    headers = {
        'Content-Type': 'application/json',
        'Authorization': access_token,
    }

    response = requests.get(url_get_device, headers=headers)
    response_json = response.json()

    temperature = response_json['traits']['sdm.devices.traits.Temperature']['ambientTemperatureCelsius']
    print('Temperature:', temperature)
    return(temperature)

#get_structure()


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read("config.ini")

    ## credentials
    project_id = config.get("BLOB_CONFIGS", "project_id")
    client_id = config.get("BLOB_CONFIGS", "client_id")
    client_secret = config.get("BLOB_CONFIGS", "client_secret")
    redirect_uri = config.get("BLOB_CONFIGS", "redirect_uri")

    access_token = config.get("BLOB_CONFIGS", "access_token")
    refresh_token = config.get("BLOB_CONFIGS", "refresh_token")

    device_name(access_token,project_id)

### TO DO : If token does not work, get the new token using resresh token 


get_structure(access_token, project_id)
device_name = device_name(access_token, project_id)

humdity = device_status_humidity(access_token, device_name)
temperature = device_status_temperature(access_token, device_name)




#connectivity = response_json['traits']['sdm.devices.traits.Connectivity']['status']
#print('Connecvity:', connectivity)
#thermostatMode = response_json['traits']['sdm.devices.traits.ThermostatMode']['mode']
#print('ThermostatMode:', thermostatMode)
#thermostatEco = response_json['traits']['sdm.devices.traits.ThermostatEco']['mode']
#print('ThermostatEco:', thermostatEco)