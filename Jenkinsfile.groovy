
def gitlabHost = '192.168.10.11'
def gitlabCredential='gitlab-jetty-key'

def projectName = 'mqtt-broker'
def releaseVersion

node('docker-common') {
    try {
        stage('checkout scm') {
            // git branch: 'dev', credentialsId: "${gitlabCredential}", url: "git@${gitlabHost}:bbc/${projectName}.git"
            checkout scm
            def pom = readMavenPom file: 'pom.xml'
            projectName     = pom.artifactId
            releaseVersion  = pom.version
            echo "ProjectName: ${projectName}, ReleaseVersion: ${releaseVersion}"
        }

        stage('package') {
            sh "mvn clean package -Dmaven.test.skip=true"
        }

        stage('archive') {
            archiveArtifacts artifacts: 'target/*.jar', fingerprint: true
        }

        stage('deploy repository') {
            sh "mvn deploy -Dmaven.test.skip=true"
        }

        stage('build downstreams') {
            build job: "${projectName}-deploy",
                    propagate: false,
                    wait: false,
                    parameters: [
                            string(name: 'ProjectName', value: "${projectName}"),
                            string(name: 'ReleaseVersion', value: "${releaseVersion}"),
                            string(name: 'DeploymentEnv', value: 'prod')
                    ]
        }

        currentBuild.result = 'SUCCESS'
    } catch(err) {
        currentBuild.result = 'FAILURE'
        throw err
    } finally {
        emailext attachLog: true, body: '$DEFAULT_CONTENT', replyTo: '$DEFAULT_REPLYTO', subject: '$DEFAULT_SUBJECT', to: '$DEFAULT_RECIPIENTS'
    }
}

