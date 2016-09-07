#!groovy
build('event_stock_client', 'docker-host') {
    checkoutRepo()
    loadBuildUtils()

    def javaLibPipeline
    runStage('load JavaLib pipeline') {
        javaLibPipeline = load("build_utils/jenkins_lib/pipeJavaLib.groovy")
    }

    def buildImageTag = "7372dc01bf066b5b26be13d6de0c7bed70648a26"
    javaLibPipeline(buildImageTag)
}