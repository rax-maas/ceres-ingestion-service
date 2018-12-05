def label = "maven-${UUID.randomUUID().toString()}"

podTemplate(label: label, containers: [
  containerTemplate(name: 'maven', image: 'maven:3-jdk-8', ttyEnabled: true, command: 'cat')
])

{
  node(label) {
    container('maven') {
      stage('checkout') {
        checkout scm
      }
      ansiColor('xterm') {
        stage('build') {
          withCredentials([file(credentialsId: 'ceres-jenkins-gcr', variable: 'GOOGLE_APPLICATION_CREDENTIALS')]) {
            sh 'mvn -DskipTests -P docker -Ddocker.image.prefix=gcr.io/ceres-dev-222017 package'
          }
        }
      }
    }
  }
}
