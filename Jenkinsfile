def gitBranch = env.BRANCH_NAME                  

pipeline {

    agent {
            label 'memphis-jenkins-small-fleet-agent'
    }

    stages {
        stage('Install GoLang') {
            steps {
                // script {
                //     // Read the version from the cloned file
                //     if (env.BRANCH_NAME ==~ /(latest)/) { 
                //         def version = readFile './version.conf'
                //     }
                //     else {
                //         def version = readFile './version-beta.conf'
                //     }                    
                //     echo "Read version from file: ${version}"
                //     // Set the version as an environment variable
                //     env.versionTag = version.trim()
                // }  
                sh 'ls -la'
                sh 'pwd'               
                sh 'wget -q https://go.dev/dl/go1.20.12.linux-amd64.tar.gz'
                sh 'sudo  tar -C /usr/local -xzf go1.20.12.linux-amd64.tar.gz'
            }
        }
        stage("Deploy Kafka.GO SDK") {
            steps {
                sh "git tag v$versionTag"
                withCredentials([sshUserPrivateKey(keyFileVariable:'check',credentialsId: 'main-github')]) {
                    sh "GIT_SSH_COMMAND='ssh -i $check' git push origin v$versionTag"
                }
                sh "GOPROXY=proxy.golang.org /usr/local/go/bin/go list -m git@github.com:memphisdev/memphis-kafka.go.git@v$versionTag"
                }
        }
      stage('Checkout to version branch'){
            when {
                expression { env.BRANCH_NAME == 'latest' }
            }        
            steps {
                withCredentials([sshUserPrivateKey(keyFileVariable:'check',credentialsId: 'main-github')]) {
                sh "git reset --hard origin/latest"
                sh "GIT_SSH_COMMAND='ssh -i $check'  git checkout -b $versionTag"
                sh "GIT_SSH_COMMAND='ssh -i $check' git push --set-upstream origin $versionTag"
                }
            }
      }        
    }

    post {
        always {
            cleanWs()
        }
        success {
            notifySuccessful()
        }
        failure {
            notifyFailed()
        }
    }
}

def notifySuccessful() {
    emailext (
        subject: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
        body: """SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':
        Check console output and connection attributes at ${env.BUILD_URL}""",
        recipientProviders: [requestor()]
    )
}
def notifyFailed() {
    emailext (
        subject: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
        body: """FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':
        Check console output at ${env.BUILD_URL}""",
        recipientProviders: [requestor()]
    )
}