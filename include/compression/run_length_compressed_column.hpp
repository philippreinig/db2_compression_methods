#pragma once

#include "compressed_column.hpp"
#include "core/global_definitions.hpp"
#include "cereal/types/vector.hpp"
#include "cereal/types/tuple.hpp"


namespace CoGaDB {
    template<class T>
    class RunLengthCompressedColumn final : public CompressedColumn<T> {
    public:
        /***************** constructors and destructor *****************/
        explicit RunLengthCompressedColumn(const std::string &name);

        ~RunLengthCompressedColumn() final;

        void insert(const ColumnType &new_Value) final;

        void insert(const T &new_value) final;

        template<typename InputIterator>
        void insert(InputIterator first, InputIterator last);

        void update(TID tid, const ColumnType &new_value) final;

        void update(PositionList &tid, const ColumnType &new_value) final;

        void remove(TID tid) final;

        // assumes tid list is sorted ascending
        void remove(PositionList &tid) final;

        void clearContent() final;

        ColumnType get(TID tid) final;

        // virtual const std::any* const getRawData()=0;
        std::string print() const noexcept final;

        [[nodiscard]] size_t size() const noexcept final;

        [[nodiscard]] size_t getSizeInBytes() const noexcept final;

        [[nodiscard]] virtual std::unique_ptr<ColumnBase> copy() const;

        void store(const std::string &path) final;
        void load(const std::string &path) final;

        T operator[](int index) final;

        /**
         * @brief Serialization method called by Cereal. Implement this method in your compressed columns to get serialization working.
         */
        template<class Archive, class F, class S>
        void serialize(Archive &archive) {
            //TODO: implement
            archive(cntElements, values);// serialize things by passing them to the archive
        }

    private:
        unsigned int cntElements = 0;
        std::vector<std::tuple<unsigned int, T>> values;
    };

    /***************** Start of Implementation Section ******************/

    template<class T>
    RunLengthCompressedColumn<T>::RunLengthCompressedColumn(const std::string &name) : CompressedColumn<T>(name), cntElements(), values() {
        //TODO: implement
        //values();            
    }

    template<class T>
    RunLengthCompressedColumn<T>::~RunLengthCompressedColumn() = default;

    template<class T>
    void RunLengthCompressedColumn<T>::insert(const ColumnType &new_Value) {
        //TODO: implement
        T new_value = std::get<T>(new_Value);     
        this->insert(new_value);            //an eigentliche insert-Funktion übergeben
        return;
    }

    template<class T>
    void RunLengthCompressedColumn<T>::insert(const T &new_value) {
        //TODO: implement
        if(values.size() == 0) {
            values.push_back(std::make_tuple(1, new_value));    //falls erstes Element, direkt hinzufügen mit 1
        } 
        else if (std::get<1>(values.back()) == new_value) {
            std::get<0>(values.back())++;                       //falls gleiches Element wie das letzte, Häufigkeit hochzählen
        } 
        else {
            values.push_back(std::make_tuple(1, new_value));   //sonst anfügen mit Häufigkeit 1
        }
        cntElements++;
        return;
    }

    template<typename T>
    template<typename InputIterator>
    void RunLengthCompressedColumn<T>::insert(InputIterator first, InputIterator last) {
        //TODO: implement
        for (InputIterator i = first; i != last; ++i) { 
            this->insert(i);                                //an eigentliche insert-Funktion übergeben
        }
        return;
    }

    template<class T>
    ColumnType RunLengthCompressedColumn<T>::get(TID tid) {
        //TODO: implement
        if (cntElements > tid) {
            unsigned int sum = 0;
            for (unsigned int i = 0; i < values.size(); i++) {
                sum += std::get<0>(values[i]);                       //Häufigkeiten aufsummieren

                if (sum > tid) {
                    return std::get<1>(values[i]);                  //falls tid kleiner als Summer der Häufigkeit wurd gesuchter Wert gefunden
                }
            }
        }
        return {};
    }

    template<class T>
    std::string RunLengthCompressedColumn<T>::print() const noexcept {
        //TODO: implement
        for (unsigned int i = 0; i < values.size(); i++) {
            std::cout << "Häufigkeit: " << std::get<0>(values[i]) << " Wert: " << std::get<1>(values[i]) << std::endl;
        }
        return {};
    }

    template<class T>
    size_t RunLengthCompressedColumn<T>::size() const noexcept {
        //TODO: implement
        return cntElements;
    }

    template<class T>
    std::unique_ptr<ColumnBase> RunLengthCompressedColumn<T>::copy() const {
        //TODO: implement
        return std::make_unique<RunLengthCompressedColumn<T>>(*this);
    }

    template<class T>
    void RunLengthCompressedColumn<T>::update(TID tid, const ColumnType &new_value) {
        //TODO: implement
            unsigned sum = 0;
            for (unsigned int i = 0; i < values.size(); i++) {
                sum += std::get<0>(values[i]);                          //Häufigkeiten aufsummieren
                if (sum > tid) {                                        //falls zu ersetzender Wert gefunden wurde:
                    if (std::get<1>(values[i]) == std::get<T>(new_value)) {          //falls neuer Wert = alter Wert, nichts tun
                        return;
                    }
                    if (std::get<0>(values[i]) == 1) {
                        std::get<1>(values[i]) = std::get<T>(new_value);             //falls alte Häufigkeit = 1, Wert einfach ersetzen
                        return;
                    }

                    std::vector<std::tuple<unsigned int, T>> new_values;

                    for (unsigned int j = 0; j < cntElements; j++) {             //sonst alle Elemente umkopieren und gewünschtes Element ersetzen
                        T value = std::get<T>(this->get(j));

                        if (tid == j) {
                            value = std::get<T>(new_value);
                        }

                        if (new_values.size() == 0) {
                            new_values.push_back(std::make_tuple(1, value));
                        } 
                        else if (std::get<1>(new_values.back()) == value) {
                            std::get<0>(new_values.back())++;
                        } 
                        else {
                            new_values.push_back(std::make_tuple(1, value));
                        }
                    }
                    values = new_values;
                    return;
                }
            }
        return;
    }

    template<class T>
    void RunLengthCompressedColumn<T>::update(PositionList &tid, const ColumnType &new_value) {
        //TODO: implement
        //for (unsigned int i = 0; i < tid->size(); i++) {
        for (unsigned int tid_: tid) {    
            this->update(tid_, new_value);               //an eigentliche update-Funktion übergeben
        }
        return;
    }

    template<class T>
    void RunLengthCompressedColumn<T>::remove(TID tid) {
        //TODO: implement
        unsigned int sum = 0;
        for (unsigned int i = 0; i < values.size(); i++) {                 
            sum += std::get<0>(values[i]);                                          //Häufigkeiten aufsummieren
            if (sum > tid) {                                                        //falls zu ersetzender Wert gefunden wurde:
                if (std::get<0>(values[i]) > 1) {                                   //falls Häufigkeit > 1, um 1 reduziere
                    std::get<0>(values[i])--;
                    cntElements--;
                    return;
                }
                if (std::get<1>(values[i - 1]) == std::get<1>(values[i + 1])) {     //falls vor und nach dem Wert der gleiche Wert steht, 
                    std::get<0>(values[i - 1]) += std::get<0>(values[i + 1]);       //  zusammenfügen und Wert entfernen
                    values.erase(values.begin() + i + 1);
                    values.erase(values.begin() + i);
                    cntElements--;
                    return;
                }
                values.erase(values.begin() + i);                                   //sonst Wert entfernen
                cntElements--;
                return;
            }
        }
        return;
    }

    template<class T>
    void RunLengthCompressedColumn<T>::remove(PositionList &tid) {
        //TODO: implement
        for (unsigned int tid_: tid) {    
            this->remove(tid_);               //an eigentliche remove-Funktion übergeben
        }
        return;
    }

    template<class T>
    void RunLengthCompressedColumn<T>::clearContent() {
        //TODO: implement
        values.clear();
        cntElements = 0;
        return;
    }

    template<class T>
    void RunLengthCompressedColumn<T>::store(const std::string &path) {
        //TODO: implement
        /*
        std::string path_(path);
        path_ += "/";
        path_ += this->name;

        std::ofstream outfile (path_.c_str(),std::ios_base::binary | std::ios_base::out);
        boost::archive::binary_oarchive oa(outfile);

        oa << values;

        outfile.flush();
        outfile.close();
        */

       
        std::string path_(path);
         path_ += this->name_;

        std::ofstream outfile(path_.c_str(), std::ofstream::binary | std::ofstream::out | std::ofstream::trunc);
        assert(outfile.is_open());
        cereal::PortableBinaryOutputArchive oarchive(outfile); // Create an output archive
        oarchive(values); 
        return;
    }

    template<class T>
    void RunLengthCompressedColumn<T>::load(const std::string &path) {
        //TODO: implement
        /*
        std::string path_(path);
        path_ += "/";
        path_ += this->name;

        std::ifstream infile (path_.c_str(),std::ios_base::binary | std::ios_base::in);
        boost::archive::binary_iarchive ia(infile);
        ia >> values;
        infile.close();

        cntElements = 0;
        for(int i = 0; i < values.size();i++){
            cntElements += std::get<0>(values[i]);
        }  
        */


        std::string path_(path);
         path_ += this->name_;

        std::ifstream infile(path_.c_str(), std::ifstream::binary | std::ifstream::in);
        cereal::PortableBinaryInputArchive ia(infile);
        ia(values);

        cntElements = 0;
        for(unsigned int i = 0; i < values.size(); i++) {
            cntElements += std::get<0>(values[i]);
        } 
        return;

    }
        

    template<class T>
    T RunLengthCompressedColumn<T>::operator[](const int index) {
        //TODO: implement
        static T val;
        if ((int) cntElements > index) {
            int sum = 0;
            for (unsigned int i = 0; i < values.size(); i++) {                   //Häufigkeiten aufsummieren
                sum += std::get<0>(values[i]);

                if (sum > index) {                                      //falls Wert gefunden, ausgeben
                    val = std::get<1>(values[i]);
                    return val;
                }
            }
        }
        return val;
    }

    template<class T>
    size_t RunLengthCompressedColumn<T>::getSizeInBytes() const noexcept {
        //TODO: implement
        return values.size() * (sizeof(T) + sizeof(int));
    }

    /***************** End of Implementation Section ******************/

}// namespace CoGaDB