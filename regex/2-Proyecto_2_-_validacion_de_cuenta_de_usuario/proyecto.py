import re

#Preguntar al usuario por el nombre y comprobar el formato
while True:
    fname = input("\nPor favor, introduzca su nombre: ")

    check = re.fullmatch(r"[A-Z][a-z]+", fname)

    if check == None:
        print("Formato incorrecto! Vuelva a intentarlo.")
        continue
    else:
        break

#Preguntar al usuario el apellido y comprobando el formato
while True:
    lname = input("\nPor favor, introduzca su apellido: ")
    
    check = re.fullmatch(r"[A-Z][a-z]+", lname)

    if check == None:
        print("Formato incorrecto! Vuelva a intentarlo.")
        continue
    else:
        break

#Preguntar al usuario la fecha de nacimiento y comprobar el formato
while True:
    date = input("\nIntroduzca su fecha de nacimiento (dd/mm/aaaa): ")
    
    check = re.fullmatch(r"(0[1-9]|[12]\d|3[01])/(0[1-9]|1[0-2])/(19[0-9][0-9]|200[01])", date)

    if check == None:
        print("Formato incorrecto! Vuelva a intentarlo.")
        continue
    else:
        break

#Preguntar al usuario por la dirección de correo electrónico y comprobar el formato
while True:
    email = input("\nIntroduzca su dirección de correo electrónico: ")
    
    # check = re.fullmatch(r".+@[a-z]+\.[a-z]{2,4}", email)
    # check = re.fullmatch(r"(.+)@([a-z]+)\.([a-z]{2,4})", email)
    check = re.fullmatch(r"(\w|\.)+@[a-z]+\.[a-z]{2,4}", email)

    if check == None:
        print("Formato incorrecto! Vuelva a intentarlo.")
        continue
    else:
        break

#Preguntar al usuario por el nombre de usuario y comprobar el formato
while True:
    user = input("\nPor favor, introduzca su nombre de usuario: ")
    
    check = re.fullmatch(r"\w{6,12}", user)

    if check == None:
        print("Formato incorrecto! Vuelva a intentarlo.")
        continue
    else:
        break

#Preguntar al usuario por la contraseña y comprobar el formato
while True:
    passw = input("\nPor favor, introduzca su contraseña: ")

    # (?=.*[A-Z]) ---> Esto se traduce en la siguiente lógica, coincide con el patrón fuera de la afirmación, 
    # si y sólo si es seguido por una letra mayúscula, ya sea inmediatamente después del primer carácter, que 
    # es la letra minúscula al principio de la contraseña o si hay otros caracteres entre la primera letra y 
    # la letra mayúscula. Así que no importa si esta letra mayúscula es la segunda letra de la contraseña o si 
    # hay otros caracteres entre la primera letra y la letra mayúscula, estas letras mayúsculas deben existir 
    # en algún lugar de la contraseña, independientemente de su ubicación real.

    # [a-zA-Z0-9$&?!%]+$ ---> Por último, la contraseña puede contener cualquiera de estos caracteres una o varias 
    # veces hasta el final de la cadena, hasta el final de la contraseña y que está señalada por esta clase de 
    # caracteres larga, seguido por el metacaracter signo más (+) al final del patrón.
    
    check = re.fullmatch(r"^[a-z](?=.{7,})(?=.*[A-Z])(?=.*\d)(?=.*[$&?!%])[a-zA-Z0-9$&?!%]+$", passw)
    
    if check == None:
        print("Formato incorrecto! Vuelva a intentarlo.")
        continue
    else:
        break

#Preguntar al usuario por el número de la tarjeta de crédito y comprobar el formato
while True:
    ccnum = input("\nPor favor, introduzca el número de su tarjeta de crédito (sin espacios): ")
    
    check = re.fullmatch(r"^(4|5)\d{15}", ccnum)
    
    if check == None:
        print("Formato incorrecto! Vuelva a intentarlo.")
        continue
    else:
        break

#Preguntar al usuario la fecha de caducidad de la tarjeta de crédito y comprobando el formato
while True:
    ccdat = input("\nPor favor, introduzca la fecha de caducidad de la tarjeta de crédito (mm/aa): ")
    
    check = re.fullmatch(r"(0[5-9]|1[0-2])/24|(0[1-9]|1[0-2])/(2[5-9]|[3-9][0-9])", ccdat)
    
    if check == None:
        print("Formato incorrecto! Vuelva a intentarlo.")
        continue
    else:
        break

#Preguntar al usuario el código de verificación de la tarjeta de crédito y comprobando el formato
while True:
    cccvc = input("\nIntroduzca el código de verificación de su tarjeta de crédito: ")
    
    check = re.fullmatch(r"\d{3}", cccvc)
    
    if check == None:
        print("Formato incorrecto! Vuelva a intentarlo.")
        continue
    else:
        break

userinfo = ["Nombre: " + fname, 
            "Apellido: " + lname, 
            "Fecha de nacimiento: " + date, 
            "Correo eléctronico: " + email, 
            "Nombre de usuario: " + user, 
            "Contraseña: " + passw, 
            "Número de tarjeta: " + ccnum, 
            "Fecha de expiración: " + ccdat, 
            "CVC: " + cccvc]

string = "\n".join(userinfo)

print("This is your user account information: \n\n" + string)