import random
import string

N = 30000000
OUT = "estudiantes.csv"

def nombre_random():
    # nombre tipo: "AnaQX" (rápido de generar)
    base = random.choice(["Ana", "Luis", "Marta", "Pablo", "Sara", "Juan", "Lucia", "Diego", "Carlos", "Elena", "Javier", "Laura", "Miguel", "Carmen", "Alberto",
                          "Raquel", "Daniel", "Paula", "Sergio", "Natalia", "David", "Andrea", "Marcos", "Irene", "Victor", "Patricia", "Alejandro", "Beatriz",
                          "Adrian", "Silvia"])
    sufijo = ''.join(random.choices(string.ascii_uppercase, k=4)) # Genera 4 letras mayúsculas aleatorias para añadir al nombre
    return base + sufijo

with open(OUT, "w", encoding = "UTF_8", newline="") as f:
    f.write("nombre, codigo_carrera,edad,indice\n")

    for i in range(N):
        nombre = nombre_random()
        codigo_carrera = random.randint(0, 100)  # Códigos de carrera entre 0 y 100
        edad = random.randint(18, 40)  # Edad entre 18 y 40 años
        indice = random.randint(0, 10000)  

        f.write(f"{nombre},{codigo_carrera},{edad},{indice}\n")

print("Archivo generado:", OUT)