package holon.streaming.processing

trait ProcFunFactory {
    def create(partition: Int): ProcFun
}
