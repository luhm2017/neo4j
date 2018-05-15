package importData

import sun.misc.ObjectInputFilter.Config


trait DataGenerator {
    def generateUsers(config:Config):Unit
}
