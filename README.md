# 2017-big-data
Репозиторий для практических домашних заданий 2017 года курса "Введение в машинное обучение для java-разработчиков" в [Технополис](https://polis.mail.ru).

### Fork
[Форкните проект](https://help.github.com/articles/fork-a-repo/), склонируйте и добавьте `upstream`:
```
$ git clone git@github.com:<username>/2017-big-data.git
Cloning into '2017-big-data'...
remote: Counting objects: 3, done.
remote: Compressing objects: 100% (2/2), done.
remote: Total 3 (delta 2), reused 3 (delta 2), pack-reused 0
Receiving objects: 100% (3/3), 10.1 KiB | 4.5 MiB/s, done.
Resolving deltas: 100% (2/2), done.
$ git remote add upstream git@github.com:polis-mail-ru/2017-big-data.git
$ git fetch upstream
From github.com:polis-mail-ru/2017-big-data
 * [new branch]      master     -> upstream/master
```

### Develop
Откройте в IDE -- [IntelliJ IDEA Community Edition](https://www.jetbrains.com/idea/) 
через File->Open, выбрав pom.xml

Время работы job WordCount без combiner составило 47m 38.473s. С использованием combiner, после добавления combiner время работы составило 31m 38.491s.

