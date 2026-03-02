# Progetto Scalable and Cloud Programming - A.A. 2025/2026
## Giulia Ferrigno

### Versioni
| Componente | Versione |
|------------|----------|
| Java       | 17       |
| Scala      | 2.12.21  |
| Spark      | 3.5.1    |
| SBT        | 1.9.7    |

### Eseguire localmente
Dalla root del progetto eseguire:

```
sbt clean compile "runMain Main path/to/dataset" 
```

### Generare JAR per Dataproc
Dalla root del progetto eseguire:

```
sbt clean assembly
```




### Struttura Repo
```
terremoti-spark/
├── src/main/scala/Main.scala    
├── build.sbt                   
├── project/                  
├── .gitignore                  
└── README.md                    
```
