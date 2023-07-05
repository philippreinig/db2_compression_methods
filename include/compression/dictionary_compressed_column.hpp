#pragma once

#include "compressed_column.hpp"
#include "core/global_definitions.hpp"
#include <map>
#include "cereal/types/map.hpp"

namespace CoGaDB {

    /*!
     *  \brief     This class represents a dictionary compressed column with type T, is the base class for all
     * compressed typed column classes.
     */
    template<class T>
    class DictionaryCompressedColumn final : public CompressedColumn<T> {
    public:
        /***************** constructors and destructor *****************/
        explicit DictionaryCompressedColumn(const std::string &name);

        ~DictionaryCompressedColumn() final;

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
        template<class Archive>
        void serialize(Archive &archive) {
            //TODO: implement
            archive(dic, values);// serialize things by passing them to the archive
        }

    private:
        std::map<int, T> dic;
        std::vector<int> values;   
    };

    /***************** Start of Implementation Section ******************/

    template<class T>
    DictionaryCompressedColumn<T>::DictionaryCompressedColumn(const std::string &name) : CompressedColumn<T>(name), dic(), values() {
        //TODO: implement
    }

    template<class T>
    DictionaryCompressedColumn<T>::~DictionaryCompressedColumn() = default;

    template<class T>
    void DictionaryCompressedColumn<T>::insert(const ColumnType &new_Value) {
        //TODO: implement
        T new_value = std::get<T>(new_Value);
        this->insert(new_value);                    //an eigentliche insert-Funktion übergeben                
        return;
    }

    template<class T>
    void DictionaryCompressedColumn<T>::insert(const T &new_value) {
        //TODO: implement
        for (auto iterator = dic.begin(); iterator != dic.end(); ++iterator) {
                if(iterator->second == new_value) {                                      //falls Wert in Wörterbuch enthalten
                    int code;
                    code = iterator->first;                                            //Wörterbucheintrag holen
                    values.push_back(code);                                            //Eintrag am Ende der Daten anfügen
                    return;
                }
        }
            if(dic.begin() == dic.end()) {
                values.push_back(1);
                dic.insert({1, new_value});
                return;
            }
            else {                                                                   //falls Wert nicht in Wörterbuch enthalten
                int lastCode = dic.rbegin()->first;
                values.push_back(lastCode+1);                                       //Neuen Wörterbucheintrag am Ende der Daten anfügen
                dic.insert({lastCode+1, new_value});                                //Neuen Wörterbucheintrag in Wörterbuch schreiben
                return; 
            }
    }
    

    template<typename T>
    template<typename InputIterator>
    void DictionaryCompressedColumn<T>::insert(InputIterator first, InputIterator last) {
        //TODO: implement
        for (InputIterator i = first; i != last; ++i){
            this->insert(i);                                    //an eigentliche insert-Funktion übergeben
        }
        return;
    }

    template<class T>
    ColumnType DictionaryCompressedColumn<T>::get(TID tid) {
        //TODO: implement
        int code = values[tid];                     //Wörterbucheintrag an Stelle tid holen
        T value;
        auto it = dic.find(code);                   //Wert holen
        value = it->second;                         
        return value;                               //Wert zurückgeben
    }

    template<class T>
    std::string DictionaryCompressedColumn<T>::print() const noexcept {
        //TODO: implement
        T value;
        for (unsigned int i = 0; i < values.size(); i++){
            auto it = dic.find(i);             
            value = it->second;               
            std::cout << value << '\n';
        }
        return {};
    }

    template<class T>
    size_t DictionaryCompressedColumn<T>::size() const noexcept {
        //TODO: implement
        return values.size();
    }

    template<class T>
    std::unique_ptr<ColumnBase> DictionaryCompressedColumn<T>::copy() const {
        //TODO: implement
        return std::make_unique<DictionaryCompressedColumn<T>>(*this);
    }

    template<class T>
    void DictionaryCompressedColumn<T>::update(TID tid, const ColumnType &new_value) {
        //TODO: implement
        if (values.size() > tid) {  
        for (auto iterator = dic.begin(); iterator != dic.end(); ++iterator) {
                if(iterator->second == std::get<T>(new_value)) {     //Wenn Code schon vorhanden                    
                int code;
                code = iterator->first;
                values[tid] = code;                     //Alten Code durch "neuen" ersetzen
                return;
                }   
        }                                        //Wenn Wert nicht Wörterbuch vorhanden  
        int lastCode = dic.rbegin()->first;
        values[tid] = lastCode+1;
        T new_value_ = std::get<T>(new_value);                 //Neuen Wörterbucheintrag am Ende der Daten anfügen
        dic.insert({lastCode+1, new_value_});                    //Neuen Wörterbucheintrag in Wörterbuch schreiben
        return;
        }
        return;
    }

    template<class T>
    void DictionaryCompressedColumn<T>::update(PositionList &tid, const ColumnType &new_value) {
        //TODO: implement
        for (unsigned int tid_: tid) {    
            this->update(tid_, new_value);               //an eigentliche update-Funktion übergeben
        }
        return;
    }

    template<class T>
    void DictionaryCompressedColumn<T>::remove(TID tid) {
        //TODO: implement
        if(values.size() > tid){
            auto code = values[tid];
            values.erase(values.begin()+(tid));
            if(std::find(values.begin(), values.end(), code) != values.end())                   //Wenn Wert noch in Wörterbuch vorhanden
            {   
                return; 
            }
            else                                                                                //falls code nicht mehr verwendet wird
            {   
                auto it = dic.find(code);                 
                dic.erase(it);
                return;
            }
        }
        return;
    }

    template<class T>
    void DictionaryCompressedColumn<T>::remove(PositionList &tid) {
        //TODO: implement
        for (unsigned int tid_: tid) {    
            this->remove(tid_);               //an eigentliche remove-Funktion übergeben
        }
        return;
    }

    template<class T>
    void DictionaryCompressedColumn<T>::clearContent() {
        //TODO: implement
        dic.clear();
        values.clear();
        return;
    }

     template<class T>
    void DictionaryCompressedColumn<T>::store(const std::string &path) {
        //TODO: implement
        std::string path_(path);
        path_ += this->name_;

        std::ofstream outfile(path_.c_str(), std::ofstream::binary | std::ofstream::out | std::ofstream::trunc);
        assert(outfile.is_open());
        cereal::PortableBinaryOutputArchive oarchive(outfile); 
        oarchive(values);

        outfile.flush();

        path_ += "dic";
        std::ofstream outfile2(path_.c_str(), std::ofstream::binary | std::ofstream::out | std::ofstream::trunc);
        assert(outfile2.is_open());
        cereal::PortableBinaryOutputArchive oarchive2(outfile2);
        oarchive2(dic);

        outfile2.flush();  
        return;
    }

    template<class T>
    void DictionaryCompressedColumn<T>::load(const std::string &path) {
        //TODO: implement
        std::string path_(path);
        path_ += this->name_;

        std::ifstream infile(path_.c_str(), std::ifstream::binary | std::ifstream::in);
        cereal::PortableBinaryInputArchive ia(infile);
        ia(values);

        path_ += "dic";
        std::ifstream infile2(path_.c_str(), std::ifstream::binary | std::ifstream::in);
        cereal::PortableBinaryInputArchive ia2(infile2);
        ia2(dic);  
        return;
    } 


    template<class T>
    T DictionaryCompressedColumn<T>::operator[](const int index) {
        //TODO: implement
        int code = values[index];                    //Wörterbucheintrag an Stelle tid holen
        T value;
        auto it = dic.find(code);                   //Wert holen
        value = it->second;                         
        return value;                               //Wert zurückgeben                   
    }

    template<class T>
    size_t DictionaryCompressedColumn<T>::getSizeInBytes() const noexcept {
        //TODO: implement
        return values.capacity() * sizeof(int) + dic.size() * (sizeof(int) + sizeof(T));
    }

    /***************** End of Implementation Section ******************/

}// namespace CoGaDB