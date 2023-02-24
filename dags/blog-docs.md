# Docs

Udemy: <https://www.udemy.com/course/the-complete-hands-on-course-to-master-apache-airflow/learn/lecture/23627004#overview>

## How to use the DockerOperator with Templates and Apache Spark

Wondering how can we kick off a Docker container and execute commands into it? In a more and more containerized world, it can be very useful to know how to interact with your Docker containers through Apache Airflow. In this article, we are going to learn how to use the DockerOperator in Airflow through a practical example using Spark. We will configure the operator, pass runtime data to it using templating and execute commands in order to start a Spark job from the container.

To learn more see you at the following tutorial (If you have downloaded the VM you will just need to copy and past the DAG):

<https://marclamberti.com/blog/how-to-use-dockeroperator-apache-airflow/>

Have a good day and if you have any questions please feel free to ask in the Q/A Section :)

## Apache Airflow with Kubernetes Executor

In this tutorial, we are going to see how to use Apache Airflow with Kubernetes Executor. If you are using Airflow in production, there is a big chance that your workload fluctuates over time. Sometimes you have many taks to execute and sometimes not at all. The problem is in both cases, your resources stay allocated to your Airflow cluster and cannot be assigned to another tool since your cluster is "static". Wasting resources is expensive in terms of time and money. Kubernetes Executor actually addresses this problem among others so let's discover this exciting new executor.

To learn more see you at the following tutorial

<https://marclamberti.com/blog/airflow-kubernetes-executor/>

Have a good day and if you have any questions please feel free to ask in the Q/A Section :)

## How to use templates and macros in Apache Airflow

Templates and Macros in Apache Airflow are the way to pass dynamic data to your DAGs at runtime. Let’s imagine that you would like to execute a SQL request using the execution date of your DAG? How can you do that? How could you use the DAG id of your DAG in your bash script to generate data? Maybe you need to know when your next DagRun will be? How could you get this value in your tasks? Well, all of these questions can be answered using macros and templates. While those two concepts work well together, we are going to first define them separately and then, in combination to show you how powerful they are. We will finish this tutorial by creating a beautiful data pipeline composing of a BashOperator, PythonOperator and PostgresOperator using templates and macros. Let’s go!

To learn more see you at the following tutorial

<https://marclamberti.com/blog/templates-macros-apache-airflow/>

Have a good day and if you have any questions please feel free to ask in the Q/A Section :)

## How to use timezones in Apache Airflow

Dealing with timezones in general can become a real nightmare if they are not correctly used. Understanding how timezones in Apache Airflow work is important since you may want to schedule your DAGs according to your local time zone, which can lead to surprises when DST (Daylight Saving Time) happens. There are some subtle notions to grasp with timezones in Apache Airflow such as aware and naive datetime objects and time delta vs cron expressions. After reading this article, **you should be able to trigger your DAGs at the time you expect whatever the time zone used**. One more thing, if you like my tutorials, you can support my work by becoming my Patron right here. No obligation but if you want to help me, I will thank you a lot. If you are new to Apache Airflow, you can check my course right here which will give you a solid introduction. Let’s begin !

To learn more, click on the following link:

<https://marclamberti.com/blog/how-to-use-timezones-in-apache-airflow/>

## How to use the BashOperator

Wondering how can you execute bash commands through Airflow ? The Airflow BashOperator does exactly what you are looking for. It is a very simple but powerful operator, allowing you to execute either a bash script, a command or a set of commands from your DAGs.

To learn more, click on the following link:

<https://marclamberti.com/blog/airflow-bashoperator/>

## Variables in Apache Airflow: The Guide

Wondering how to deal with variables in Apache Airflow? Well you are at the right place. In this tutorial, you are going to learn everything you need about the variables in Airflow. What are they, how they work, how can you define them, how to get them, best practices and more.

To learn more, click on the following link:

<https://marclamberti.com/blog/variables-with-apache-airflow/>

## Best Practices in Apache Airflow (part 1)

Since I started creating courses a year ago, I got so many messages asking me what are the best practices in Apache Airflow. As engineer, we always seek for the best ways to apply what we learn while being constantly improving ourselves. In this series of tutorial, I would like to share with you everything I learned so far to really make Airflow shine in your data ecosystem. You will learn how to create better DAGs, how to optimise Airflow, what you should care about and much more.

To learn more, click on the link:

<https://marclamberti.com/blog/apache-airflow-best-practices-1/>

## Running Apache Airflow on a multi-nodes Kubernetes cluster locally

Wouldn’t be convenient to be able to run Apache Airflow locally with the Kubernetes Executor on a multi-node Kubernetes cluster? That’s could be a great way to test your DAGs and understand how Apache Airflow works in a Kubernetes environment isn’t it? Well that’s exactly what we are going to do here. I will show you step by step, how to quickly set up your own development environment and start running Apache Airflow locally on Kubernetes.

To learn more, click on the following link:

<https://marclamberti.com/blog/running-apache-airflow-locally-on-kubernetes/>

## The PostgresOperator: All you need to know

One of the first operators I discovered with Airflow was the PostgresOperator. The PostgresOperator allows you to interact with your Postgres database. Whether you want to create a table, delete records, insert records, you will use the PostgresOperator. Nonetheless, you will quickly be faced to some questions. How can I get records from it? How can I pass parameters to my SQL requests? And others that we are going to answer in this article.

Click on the link: <https://marclamberti.com/blog/the-postgresoperator-all-you-need-to-know/>
